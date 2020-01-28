#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <math.h>
#include <string.h>

SM_FileHandle* MasterFH;
int maxKeys;
TreeNode* TreeRoot;
TreeNode* TreeScan;
int MNodeCount;
int MEntryCount;	//sum of keys of all nodes.
DataType MasterKeyType;

/*
typedef struct TreeNode
{
	TreeNode* parent;
	int nodeSize;
	bool isLeaf;
	//Each TreeNode has a list of Keys, RIDs, and child pointers.
	int *keys;
	RID *ids;
	TreeNode **nexts;
} TreeNode;
*/


RC createBtree (char *idxId, DataType keyType, int n)
{
	TreeRoot = malloc(sizeof(TreeNode));
	TreeRoot->parent = NULL;
	TreeRoot->nodeSize = 0;
	TreeRoot->isLeaf = true;	//As the single node, it's both THE root and THE leaf.
	maxKeys = n;
	
	TreeRoot->key = malloc(sizeof(int) * maxKeys+1);		//One spot for pseudo-key. If that spot is filled, SPLIT IMMEDIATELY
	TreeRoot->id = malloc(sizeof(int) * maxKeys+1);
	TreeRoot->next = malloc(sizeof(TreeNode) * maxKeys+1);
	for(int i = 0; i < maxKeys+1; i++)
	{
		TreeRoot->key[i] = NULL;
		TreeRoot->id[i].page = NULL;
		TreeRoot->id[i].slot = NULL;
		TreeRoot->next[i] = NULL;
	}

	MNodeCount = 1;				//Only a root
	MEntryCount = 0;			//Root is empty
	MasterKeyType = keyType;	//Not needed? ONly support ints.

	createPageFile(idxId);
	MasterFH = malloc(sizeof(SM_FileHandle));
	openPageFile(idxId, MasterFH);
	appendEmptyBlock(MasterFH);
	writeBlock(0, MasterFH, (SM_PageHandle)TreeRoot);
	free(MasterFH);
 	printf("max keys: %d\n",n);
	return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId)
{
	TreeRoot = malloc(sizeof(TreeNode));
	MasterFH = malloc(sizeof(SM_FileHandle));
	openPageFile(idxId, MasterFH);

	TreeNode* temp = malloc(4096);
	readBlock(0, MasterFH, temp);
	TreeRoot = (TreeNode*)temp;
	// printf("aaaaaa%d\n", TreeRoot->nodeSize);

	return RC_OK;
}

RC closeBtree (BTreeHandle *tree) { return closePageFile(&MasterFH); }

RC deleteBtree (char *idxId) 
{ 
	MEntryCount = 0;
	MNodeCount = 0;
	return destroyPageFile(idxId); 
}


// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result)
{
	*result = MNodeCount-1;
	return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result)
{
	*result = MEntryCount;
	return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result)
{
	*result = MasterKeyType;
	return RC_OK;
}

RC findKey (BTreeHandle *tree, Value *key, RID *result)
{	
	if(TreeRoot == NULL) { return RC_ERROR; }
	TreeNode* temp = malloc(sizeof(TreeNode));

	temp = TreeRoot;

	//Using the node tracker(temp), go to the specific leaf node where the key would be.
	while(temp->isLeaf == false) //Keep going until we reach the leaf
	{
		for(int i = 0; i < temp->nodeSize; i++)
		{
			if(temp->key[i] > key->v.intV) { temp = temp->next[i]; break; }
			else if(i == temp->nodeSize -1) { temp = temp-> next[i+1]; break; }
		}
	}
	//MARKER: Now the node tracker(temp) is pointing at the leaf node where the key would be.

	//Loop through the keys in the node, if there is a match, return.
	//If by the end of the keys list, still no match, return error.
	for(int i = 0; i <temp->nodeSize; i ++)
	{
		if(temp->key[i] == key->v.intV)
		{
			(*result).page = temp->id[i].page;
			(*result).slot = temp->id[i].slot;
			return RC_OK;
		}
		//printf("%d\n\n",RC_IM_KEY_NOT_FOUND);
		return RC_IM_KEY_NOT_FOUND;
	}
}



RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
	printf("key to insert: %d  - node count: %d, entry count: %d\n",key->v.intV, MNodeCount, MEntryCount);
	//If this is the FIRST ever insertion, into an empty node, insert and return.
	if(MEntryCount == 0)
	{
		TreeRoot->nodeSize = 1;
		TreeRoot->key[0] = key->v.intV;
		TreeRoot->id[0].page = rid.page;
		TreeRoot->id[0].slot = rid.slot;
		TreeRoot->next[0] = NULL;
		MNodeCount = 1;
		MEntryCount += 1;
		printf("checkpoint 1\n");
		return RC_OK;
	}

	TreeNode* current = TreeRoot;
	//If this is not the first insertion, then travel to the node where the key SHOULD be.
	while(current->isLeaf == false)
	{
		for(int i = 0; i < current->nodeSize; i++)
		{
			if( key->v.intV < current->key[i]) { current = current->next[i]; break; }
			if( i == current->nodeSize-1 ) { current = current->next[i+1]; break; }
		}
	}
	//MARKER: we're now at the leaf node where the key is supposed to be.

	//Two situations here:
	//1. Target leaf node has enough space for the new key. Basic sorted insertion is good.
	//2. Overflow.
	//Case 1:
	if(current->nodeSize < maxKeys) 
	{ 
		sortedInsertionIntoNode(current, key->v.intV, rid); 
		// printf("addition into nonempty, nonfull node.\n");
		return RC_OK;
	}
	//Case2:
	//Create a new node, move (n+1)/2 of the largest entries of current node into the new node.
	//set pointers of current node, new node.
	//Update the parent's values and pointers.
	//Recursion: keep spliting the parent nodes.
	//Recursion termination: reached the root, create a new root.
	if(current->nodeSize == maxKeys)
	{
		// printf("overflow insert\n");
		sortedInsertionIntoNode(current, key->v.intV, rid);
		//MARKER: Now there is an overflow, an extra keey in the node.

		TreeNode* newNode = malloc(sizeof(TreeNode));
		newNode->parent = NULL;		//new node is sibling of current node.
		newNode->nodeSize = 0;
		newNode->isLeaf = true;
		newNode->key = malloc(sizeof(int) * maxKeys+1);
		newNode->id = malloc(sizeof(int) * maxKeys+1);
		newNode->next = malloc(sizeof(TreeNode) * maxKeys+1);
		for(int i = 0; i < maxKeys+1; i++)
		{
			newNode->key[i] = NULL;
			newNode->id[i].page = NULL;
			newNode->id[i].slot = NULL;
			newNode->next[i] = NULL;
		}
		MNodeCount += 1;
		//MARKER: new node is created.

		//Shift (n+1)/2 of the largest entries of current node into new node.
		//Since insertion was sorted, this will be the (n+1)/2 keys at the tail end.
		int looper = (maxKeys+1)/2;

		for(int i = 0, k = (maxKeys+1)-looper; i < looper, k < maxKeys+1; i++,k++)
		{
			newNode->key[i] = current->key[k];
			newNode->id[i].page = current->id[k].page;
			newNode->id[i].slot = current->id[k].slot;
			newNode->next[i] = current->next[k];
			newNode->nodeSize += 1;
			current->nodeSize -= 1;
			current->key[k] = NULL;
			current->id[k].page = NULL;
			current->id[k].slot = NULL;
			current->next[k] = NULL;
		}

		newNode->next[maxKeys] = current->next[maxKeys];
		current->next[maxKeys] = newNode;
		// printf("reached here\n");
		//MARKER: Now the elevments are where they're supposed to be. And sibling pointers are set.
		
		//Take the smallest element of the new node, and push it up to the parent.
		//Since insertions are sorted, this is element[0].
		//If the current node is the root, create a new root.
		if(current->parent	== NULL) //only root nodes have null parents.
		{
			TreeNode* newRoot = malloc(sizeof(TreeNode));
			newRoot->parent = NULL;
			newRoot->nodeSize = 1;
			newRoot->isLeaf = false;
			newRoot->key = malloc(sizeof(int) * maxKeys+1);		//One spot for pseudo-key. If that spot is filled, SPLIT IMMEDIATELY
			newRoot->id = malloc(sizeof(int) * maxKeys+1);
			newRoot->next = malloc(sizeof(TreeNode) * maxKeys+1);
			for(int i = 1; i < maxKeys+1; i++)
			{
				newRoot->key[i] = NULL;
				newRoot->id[i].page = NULL;
				newRoot->id[i].slot = NULL;
				newRoot->next[i] = NULL;
			}
			newRoot->key[0] = newNode->key[0];
			newRoot->id[0].page = newNode->id[0].page;
			newRoot->id[0].slot = newNode->id[0].slot;
			newRoot->next[0] = current;
			newRoot->next[1] = newNode;
			TreeRoot = newRoot;
			//new root is created, with one entry in it, set counters, and pointers.

//Currently at leaf nodes:
//After an overflow occurs, a new sibling node to the overflowed node is created.
//All the keys have been correctly set. Now, since these nodes are siblings, they must have the same parent.
//So, both nodes' paresent pointers are set accordingly, to the same parent.
			current->parent = newRoot;
			//newNode->parent = current->parent;////
//HOWEVER, when setting the parent pointer of the newly created root, there is a segmentation fault.

			MNodeCount += 1;////
			// MEntryCount += 1;
			// printf("aaa& ddddd   %d,%d,...%d\n",TreeRoot->key[0],newNode->key[0], current->key[0]);
			return RC_OK;
		}
		else  	//if current is not root, recursive modify parents.
		{
			recursiveInsertParent(newNode->key[0], newNode->id[0], current->parent, newNode);
		}
	}
	// printf("node count: %d, entry count: %d\n",MNodeCount,MEntryCount);
	return RC_OK;
}


TreeNode* findParentNode(TreeNode* searcher, TreeNode* child)
{
	TreeNode* result;
	if(searcher->isLeaf) { return NULL; }

	for(int i = 0; i < searcher->nodeSize+1; i++)
	{
		if(searcher->next[i] == child) { result = searcher; return searcher; }
		else { searcher = findParentNode(searcher->next[i],child); }
	}
	return searcher;
}



//When a full node is split, its parent node must be modified.
//If the target non-leaf node is NOT full, then do sorted insertion.
//If the target non-leaf node is full, then split it into two, set its pointers accordingly.
//After the split. repeat what was done during the insertion.
//CHeck if its parent is null, if so, create a new root. If not. recursive call.
RC recursiveInsertParent(int key, RID rid, TreeNode* target, TreeNode* newchild)
{
	//printf("here\n");
	if(target->nodeSize < maxKeys)
	{
		sortedInsertionIntoNode(target,key,rid);
	}
	else 	//If target parent node is full.
	{
		
	}


}







//Go through a node's data list, once an element has a key larger than the given one,
//push it back, and insert the given key into that spot.
//Once a key is added, the node entry count and master entry count increments.
RC sortedInsertionIntoNode(TreeNode* node, int key, RID rid)
{
	for(int i = maxKeys; i > 0; i ++)	//include the virtual key
	{
		if(node->key[i] == NULL || node->key[i] > key)
		{
			node->key[i] = node->key[i-1];
			node->id[i].page = node->id[i-1].page;
			node->id[i].slot = node->id[i-1].slot;
			node->next[i] = node->next[i-1];			//Precautional, not necessary, since all insertions are into leaf nodes, with not next pointers.
		}

		if(node->key[i] < key || node->key[i] != NULL)
		{
			node->key[i] = key;
			node->id[i].page = rid.page;
			node->id[i].slot = rid.slot;
			node->next[i] = NULL;			//Precautional, not necessary, since all insertions are into leaf nodes, with not next pointers.
			break;
		}
	}
	node->nodeSize += 1;
	MEntryCount += 1;
	// printf("aaaa %d   -   %d\n\n",MEntryCount,maxKeys);
	return RC_OK;
}










RC deleteKey (BTreeHandle *tree, Value *key)
{
	return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
	return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result)
{
	return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle)
{
	indexNum = 0;
	return RC_OK;
}


// debug and test functions
char *printTree (BTreeHandle *tree)
{
	return RC_OK;
}
RC initIndexManager (void *mgmtData)
{
	return RC_OK;
}

RC shutdownIndexManager ()
{
	return RC_OK;
}