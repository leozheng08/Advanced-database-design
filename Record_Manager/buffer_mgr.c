#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "time.h"
#include "limits.h"

int curr = 0;
int pagesWritten = 0;
int pagesRead = 0;
int masterTimer = 0;

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
	curr = 0;
	pagesWritten = 0;
	pagesRead = 0;
	masterTimer = 0;
	
	PageFrame* PFrame = malloc(sizeof(PageFrame) * numPages);
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
	bm->mgmtData = PFrame;
	bm->BMFH = malloc(sizeof(SM_FileHandle));
	openPageFile((char*)pageFileName, bm->BMFH);	
	for(int i = 0; i < numPages; i++)
	{
		PageFrame *temp = &PFrame[i];
		temp->pageNumber = NO_PAGE;
		temp->dirty = 0;
		temp->pin = 0;
		temp->timeMarker = masterTimer;
		temp->data = NULL;
	}
	return RC_OK;
}

//Buffer pool shutdown checklist:
//1. Commit all changes into the disk.
//2. Abort shutdown if any page in the pool is currently being used.
//3. Free memories designated to the buffer pool.
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	if (forceFlushPool(bm) != RC_OK) { return RC_ERROR; }

	PageFrame* PFrame = (PageFrame*)bm->mgmtData;
	for(int i = 0; i < bm->numPages; i++)	//Check if there are pinned pages
	{
		if(PFrame[i].pin != 0) { return RC_PINNED; }
	}
	free(PFrame);
	bm->mgmtData = NULL;
	return RC_OK;
}

//Procedure:
//Find all unpinned pages that are dirty.
//Clean pages, not how many pages changed.
RC forceFlushPool(BM_BufferPool *const bm)
{
	PageFrame* PFrame = (PageFrame*)bm->mgmtData;
	
	for(int i = 0; i < bm->numPages; i++)
	{		
		PageFrame* temp = &PFrame[i];
		if(isUnpinned(temp) && isDirty(temp))
		{
			temp->dirty = 0;
			writeToDisk(bm, temp);
		}
	}	
	return RC_OK;
}

void writeToDisk(BM_BufferPool *const bm, PageFrame *page)
{
	SM_FileHandle FHandle;
	openPageFile(bm->pageFile, &FHandle);
	writeBlock(page->pageNumber, &FHandle, page->data);
	pagesWritten += 1;
}

bool isUnpinned(PageFrame* PFrame)
{
	if(PFrame->pin == 0) { return true; }
	return false;
}
bool isDirty(PageFrame* PFrame)
{
	if(PFrame->dirty == 1) { return true; }
	return false;
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame* target = findTargetPage(bm, page);
	if (target == NULL) { return RC_PAGE_NOT_FOUND; }
	target->dirty = 1;
	return RC_OK;
}

PageFrame* findTargetPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame* PFrame = (PageFrame*)bm->mgmtData;
	for(int i = 0; i < bm->numPages; i++)
	{
		if(PFrame[i].pageNumber == page->pageNum) { return &PFrame[i]; }
	}
	return NULL;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{	
	PageFrame *target = findTargetPage(bm, page);
	if (target == NULL) { return RC_PAGE_NOT_FOUND; }
	target->pin -=1;
	return RC_OK;
}


//Procedure:
//1. Find target page, make sure it exists.
//2. Commit its changes to the disk, if any.
//3. NOte its dirty bit and pages written.
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame* target = findTargetPage(bm, page);
	if (target == NULL) { return RC_PAGE_NOT_FOUND; }
	
	SM_FileHandle FHandle;
	openPageFile(bm->pageFile, &FHandle);
	if(writeBlock(target->pageNumber, &FHandle, target->data) != RC_OK) { return RC_WRITE_FAILED; }
	else
	{
		target->dirty = 0;
		pagesWritten += 1;
		return RC_OK;
	}
}


//Conditions:
//1. THe page is already in the buffer, load it.
//2. Buffer is full, cue replacemnet strategies.
//3. Buffer has available spaces, simple insertion, then process it.
extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	PageFrame* PFrame = (PageFrame*)bm->mgmtData;
	PageFrame* vacantPage = NULL;
	//int vacantIndex = -1;
	for (int i = 0; i < bm->numPages; i++)
	{
		if(PFrame[i].pageNumber == NO_PAGE) { vacantPage = &PFrame[i]; break; }
	}

//Condition 1: The page is already in the buffer.
//Load it, and note changes.
	for(int i = 0; i < bm->numPages; i++)
	{
		if(PFrame[i].pageNumber == pageNum)
		{
			//load
			page->data = PFrame[i].data;
			page->pageNum = pageNum;
			//note changes
			PFrame[i].pin +=1 ;
			PFrame[i].timeMarker = masterTimer;
			masterTimer++;
			return RC_OK;
		}
	}
//Condition 2: Thre ar eno more spaces in the buffer, cue replacement policies.
	if(vacantPage == NULL) //If Buffer is full, no vacant pages left.
	{
		PageFrame* new = (PageFrame*)malloc(sizeof(PageFrame));		
		
		// Reading page from disk and initializing page frame's content in the buffer pool
		new->data = (SM_PageHandle)malloc(PAGE_SIZE);
		readFromDisk(bm, new, pageNum);
		//load
		page->pageNum = pageNum;
		page->data = new->data;		
		
		//create new page with corresponding info.
		new->pageNumber = pageNum;
		new->dirty = 0;		
		new->pin = 1;
		curr += 1;
		masterTimer += 1;
		new->timeMarker = masterTimer;	

		if(bm->strategy == RS_FIFO) { if(strat_FIFO(bm, new) == NULL) { return RC_ERROR; } }
		if(bm->strategy == RS_LRU) { if(strat_LRU(bm, new) == NULL) { return RC_ERROR; } }
		return RC_OK;						
	}	
 
//Condition 3: If there are spaces left in the buffer, simple insertion.
  	for(int i = 0; i < bm->numPages; i++)
	{
		if(PFrame[i].pageNumber == NO_PAGE)
		{	
			PFrame[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
			readFromDisk(bm, &PFrame[i], pageNum);
			//load
			page->pageNum = pageNum;
			page->data = PFrame[i].data;	
			//Simple insertion into buffer
			PFrame[i].pageNumber = pageNum;
			PFrame[i].pin += 1;
			if(i == 0) { curr = i; } else { curr += 1;}
			PFrame[i].timeMarker = ++masterTimer;				
			return RC_OK;
		}
	}
	return RC_ERROR;
}

void readFromDisk(BM_BufferPool *const bm, PageFrame *const page, const PageNumber pageNum)
{
	SM_FileHandle FHandle;
	openPageFile(bm->pageFile, &FHandle);
	readBlock(pageNum, &FHandle, page->data);
	pagesRead += 1;
}

//FIFO, first in first out strategy.
//Follow logic of Notes page 34.
PageFrame* strat_FIFO(BM_BufferPool *const bm, PageFrame *page)
{
	int next = curr % bm->numPages;
	PageFrame* PFrame = (PageFrame*)bm->mgmtData;
	//PageFrame* returnValue = NULL;

	for(int i = 0; i < bm->numPages; i++)
	{
		PageFrame* temp = &PFrame[next];

		//If the target's being used by a user, look at its next node.
		//If the next node is the last one, turn it back around to the first node.
		if(temp->pin >0) { if(++next == bm->numPages) { next = 0; } }
		else //If THE NEXt node is fully unpinned.
		{
			if(temp->dirty == 1) //if dirty, clean it.
			{ 
				writeToDisk(bm, temp);
				temp->dirty = 0; //now its cleanned
			} 
			//Load it.
			temp->pageNumber = page->pageNumber;
			temp->pin = page->pin;
			temp->dirty = page->dirty;
			temp->data = page->data;
			return temp;
		}
	}
	return NULL;
}


PageFrame* LRUhelper(BM_BufferPool *const bm)
{
	PageFrame* PFrame = (PageFrame*)bm->mgmtData;

	int position = -1;
	int timeStamp = INT_MAX;
//	long timeStamp = LONG_MAX;

	for(int i = 0; i < bm->numPages; i++)
	{
		if (PFrame[i].pin == 0)
		{
			if (PFrame[i].timeMarker < timeStamp)
			{
				position = i;
				timeStamp = PFrame[i].timeMarker;
			}
		}
	}

	if (position == -1) { return NULL; }
	else { return &PFrame[position];}
}

PageFrame* strat_LRU(BM_BufferPool *const bm, PageFrame *page)
{
 	PageFrame* target = LRUhelper(bm);
 	if (target == NULL) { return target; }
 	if(isDirty(target)) { writeToDisk(bm, target); }

	target->data = page->data;
	target->pageNumber = page->pageNumber;
	target->dirty = page->dirty;
	target->pin = page->pin;
	target->timeMarker = page->timeMarker;
	return target;
}


PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber* returnValue = malloc(bm->numPages * sizeof(PageNumber));
	PageFrame* PFrame = (PageFrame*) bm->mgmtData;
	
	for(int i = 0; i < bm->numPages; i++)
	{
		returnValue[i] = PFrame[i].pageNumber;
	}
	return returnValue;
}

//Retrieves dirty statuses as an array.
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool* returnValue = malloc(bm->numPages * sizeof(bool));
	PageFrame* PFrame = (PageFrame*)bm->mgmtData;
	            //does int 1 and 0 cast automatically? check.
	for(int i = 0; i < bm->numPages; i++) { returnValue[i] = PFrame[i].dirty; }	
	return returnValue;
}

//Retrieves pin statuses as an array.
int *getFixCounts (BM_BufferPool *const bm)
{
	int* returnValue = malloc(bm->numPages * sizeof(int));
	PageFrame* PFrame= (PageFrame*)bm->mgmtData;
	
	for(int i = 0; i <bm->numPages; i++) { returnValue[i] = (PFrame[i].pin); }
	return returnValue;
}

int getNumReadIO (BM_BufferPool *const bm)
{
//	printf("%d\n", pagesRead);
	return (pagesRead);
}

int getNumWriteIO (BM_BufferPool *const bm) { return (pagesWritten); }
