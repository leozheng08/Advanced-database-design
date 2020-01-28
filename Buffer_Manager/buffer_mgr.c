#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>



//claim the data structure of a frame node which contains one page content;
typedef struct frameNode{
    int pageNum;
    int frameNum;
    int dirtyBit;
    int fixCount;
    char *data;
    struct frameNode *next;
    struct frameNode *previous;
}frameNode;

//claim the data structure of a frame list which consists buffer pool;
typedef struct frameList{
    frameNode *head;
    frameNode *tail;
}frameList;

//claim the data structure of the buffer pool which is consisted of frame list;
typedef struct bufferPool{

    int numOfFrames;
    int numOfReads;
    int numOfWrites;
    int numOfPins;
    void *startData;

    int pageToFrame[1000];
    //    In every page location of an Array contains the value of FrameNum;
    int frameToPage[100];
    //    In every frame location of an Array contains the value of pageNum;
    bool dirtyFlags[100];
    //    In every frame location of an Array contains the value of dirtyBit;
    int fixedCounts[100];
    //    In every frame location of an Array contains the value of fixCount;
    int pageHistory[1000][10];
    // an array used for LRU_K replacement strategy;
    frameList *frames;
}bufferPool;

//a new node creation for frame list;
frameNode *newNodeCreation(){

    frameNode *node = malloc(sizeof(frameNode));
    node->pageNum = -1;
    node->frameNum = 0;
    node->dirtyBit = 0;
    node->fixCount = 0;
    node->data =  malloc(PAGE_SIZE*sizeof(SM_PageHandle));
    node->next = NULL;
    node->previous = NULL;
    return node;
}

// use change node instead of the head node of the frame list;
void changeHeadNode(frameList **list, frameNode *changeNode){

    frameNode *head = (*list)->head;

//    when the change node is equal to the head node or the head node is null or the change node is null;
    if(changeNode == (*list)->head || head == NULL || changeNode == NULL){
        return;
    }
// when the change node is equal to the tail node of the frame list;
    else if(changeNode == (*list)->tail){
        frameNode *temp = ((*list)->tail)->previous;
        temp->next = NULL;
        (*list)->tail = temp;
    }
//    when the change node is equal to the any node in the middle of the frame list
    else{
        changeNode->previous->next = changeNode->next;
        changeNode->next->previous = changeNode->previous;
    }

    changeNode->next = head;
    head->previous = changeNode;
    changeNode->previous = NULL;

    (*list)->head = changeNode;
    (*list)->head->previous = NULL;
    return;
}

// read the pageNum page content of the file into the frame node current(pin Page);
RC updateNextNewFrame(BM_BufferPool *const bm, frameNode *current, BM_PageHandle *const page, const PageNumber pageNum){

    SM_FileHandle file;
    bufferPool *info = (bufferPool *)bm->mgmtData;
    RC status;

    status = openPageFile ((char *)(bm->pageFile), &file);
    if (status != RC_OK){
        return status;
    }

//    when the dirtyBit of the frame node current is equal to 1, which means should write the content of current into file firstly;
    if(current->dirtyBit == 1){
        status = ensureCapacity(pageNum, &file);
        if(status != RC_OK){
            return status;
        }
        status = writeBlock(current->pageNum,&file, current->data);
        if(status != RC_OK){
            return status;
        }
        (info->numOfWrites)++;

    }

    (info->pageToFrame)[current->pageNum] = NO_PAGE;

    status = ensureCapacity(pageNum, &file);
    if(status != RC_OK){
        return status;
    }
//    read the pageNumth page content into frame node current;
    status = readBlock(pageNum, &file, current->data);
    if(status != RC_OK){

        return status;
    }

    page->pageNum = pageNum;
    page->data = current->data;

    (info->numOfReads)++;


    current->dirtyBit = 0;
    current->fixCount = 1;
    current->pageNum = pageNum;

    (info->pageToFrame)[current->pageNum] = current->frameNum;
    (info->frameToPage)[current->frameNum] = current->pageNum;

    status = closePageFile(&file);

    return status;

}

//search the frame node whose pageNumber is equal to pageNum;
frameNode *searchNodeByPageNum(frameList *list, const PageNumber pageNum){

    frameNode *currentNode = list->head;

    while(currentNode != NULL){
        if(currentNode->pageNum == pageNum){
            return currentNode;
        }
        currentNode = currentNode->next;
    }

    return NULL;
}

//search the frame Node whose pageNumber is equal to pageNum;
frameNode *searchPageInMemory(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum){

    frameNode *current;
    bufferPool *info = (bufferPool *)bm->mgmtData;

    if((info->pageToFrame)[pageNum] != NO_PAGE){
        if((current = searchNodeByPageNum(info->frames, pageNum)) == NULL){
            return NULL;
        }

        /* return data and details of page*/
        page->pageNum = pageNum;
        page->data = current->data;

        current->fixCount++;

        return current;
    }
    return NULL;
}


// pin page by using FIFO first replacement strategy;
RC pinPageUsingFIFOStrategy (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    frameNode *current;
    bufferPool *info = (bufferPool *)bm->mgmtData;

    // Check whether there is a frame node which contains the pageNum content;
    current = searchPageInMemory(bm, page, pageNum);
    if(current != NULL){
        return RC_OK;
    }
    //check whether the buffer pool is full; if not put the frame node into the end of the frame list
    if((info->numOfFrames) < bm->numPages){
        current = info->frames->head;
        int i = 0;

        while(i < info->numOfFrames){
            current = current->next;
            ++i;
        }
        (info->numOfFrames)++;
        changeHeadNode(&(info->frames), current);
    }
    else{

        // if the buffer pool  is filled out, replace frame node  which is come first in memory;
        current = info->frames->tail;
        //if fixCount is not equal to 0, it means someone is using this node, then go to the next;
        while(current != NULL && current->fixCount != 0){
            current = current->previous;
        }

        if (current == NULL){
            return RC_NO_MORE_SPACE_IN_BUFFER;
        }

        changeHeadNode(&(info->frames), current);
    }

    // Update new page to frame
    RC status =updateNextNewFrame(bm, current, page, pageNum);
    if(status  != RC_OK){
        return status;
    }else
        return RC_OK;
}

// pin page by using LRU replacement strategy;
RC pinPageUsingLRUStrategy (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    frameNode *current;
    bufferPool *info = (bufferPool *)bm->mgmtData;

    // Check whether there is a frame node which contains the pageNum content;
    current = searchPageInMemory(bm, page, pageNum);
    if(current != NULL){
    //  use the look up node instead of the head node of the frame list;
        changeHeadNode(&(info->frames), current);
        return RC_OK;
    }

    //check whether the buffer pool is full; if not put the frame node into the end;

    if((info->numOfFrames) < bm->numPages){
        current = info->frames->head;

        int i = 0;
        while(i < info->numOfFrames){
            current = current->next;
            ++i;
        }
        (info->numOfFrames)++;
    }
        // if the buffer pool is filled out, replace frame node  which is least recently used;
    else{
        current = info->frames->tail;

//        when the fixcount of the frame node is not equal to 0, go to the next;
        while(current != NULL && current->fixCount != 0){
            current = current->previous;
        }

        if (current == NULL){
            return RC_NO_MORE_SPACE_IN_BUFFER;
        }
    }

    //  put the recently used frame to the head node of the frame list as per LRU strategy.
    changeHeadNode(&(info->frames), current);

    RC status = updateNextNewFrame(bm, current, page, pageNum);

    if(status != RC_OK)
        return status;
    else
        return RC_OK;
}

// pin page by using LRU_K replacement strategy;
RC pinPageUsingLRU_KStrategy (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    frameNode *current;
    bufferPool *info = (bufferPool *)bm->mgmtData;
    int K = (int)(info->startData);
    int i;
    (info->numOfPins)++;

    // Check whether there is a frame node which contains the pageNum content;
    current = searchPageInMemory(bm, page, pageNum);
    if(current != NULL){

        for(i = K-1; i>0; i--){
            info->pageHistory[current->pageNum][i] = info->pageHistory[current->pageNum][i-1];
        }

        info->pageHistory[current->pageNum][0] = info->numOfPins;

        return RC_OK;
    }


    //check whether the buffer pool is full; if not put the frame node into the end;
    if((info->numOfFrames) < bm->numPages){
        current = info->frames->head;

        int i = 0;
        while(i < info->numOfFrames){
            current = current->next;
            ++i;
        }
        (info->numOfFrames)++;
    }

    else{
        // if the buffer pool is filled out, replace frame node  which is least recently used and has maximum value of backward K-distance;

        frameNode *currentNode;
        currentNode = info->frames->head;
        int dist, max_dist = -1;
        //    find the node which has largest distance;
        while(currentNode != NULL){
            if(currentNode->fixCount == 0 && info->pageHistory[currentNode->pageNum][K] != -1){

                dist = info->numOfPins - info->pageHistory[currentNode->pageNum][K];

                if(dist > max_dist){
                    max_dist = dist;
                    current = currentNode;
                }
            }
            currentNode = currentNode->next;
        }

        // if reached to end , it means no frame with fixed count 0 is available.
        if(max_dist == -1){
            currentNode = info->frames->head;

            while(currentNode->fixCount != 0 && currentNode != NULL){
                dist = info->numOfPins - info->pageHistory[currentNode->pageNum][0];
                if(dist > max_dist){
                    max_dist = dist;
                    current = currentNode;
                }
                currentNode = currentNode->next;
            }

            // if max_dist is -1 then no frame with fixed count 0 is available.
            if (max_dist == -1){
                return RC_NO_MORE_SPACE_IN_BUFFER;
            }
        }
    }

    RC status = updateNextNewFrame(bm, current, page, pageNum);

    if(status  != RC_OK){
        return status;
    }

    for(i = K-1; i>0; i--){
        info->pageHistory[current->pageNum][i] = info->pageHistory[current->pageNum][i-1];
    }
    info->pageHistory[current->pageNum][0] = info->numOfPins;

    return RC_OK;
}




// inititialize Buffer pool;
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *startData)
{
    int i;
    SM_FileHandle file;

    if(numPages <= 0){
        return RC_INVALID_BM;
    }

    if (openPageFile ((char *)pageFileName, &file) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }

    // initialize all the attributes
    bufferPool *info = malloc(sizeof(bufferPool));

    info->numOfFrames = 0;
    info->numOfReads = 0;
    info->numOfWrites = 0;
    info->startData = startData;
    info->numOfPins = 0;

    // initialize all the related array;
    memset(info->frameToPage,NO_PAGE,100*sizeof(int));
    memset(info->pageToFrame,NO_PAGE,1000*sizeof(int));
    memset(info->dirtyFlags,NO_PAGE,100*sizeof(bool));
    memset(info->fixedCounts,NO_PAGE,100*sizeof(int));
    memset(info->pageHistory, -1, sizeof(&(info->pageHistory)));

    // initialize a empty frame list;
    info->frames = malloc(sizeof(frameList));

    info->frames->head = info->frames->tail = newNodeCreation();

    for(i = 1; i<numPages; ++i){
        info->frames->tail->next = newNodeCreation();
        info->frames->tail->next->previous = info->frames->tail;
        info->frames->tail = info->frames->tail->next;
        info->frames->tail->frameNum = i;
    }

    bm->numPages = numPages;
    bm->pageFile = (char*) pageFileName;
    bm->strategy = strategy;
    bm->mgmtData = info;


    closePageFile(&file);
    return RC_OK;
}


//write the content of all frame node which has being used by someone to the file disk;
RC forceFlushPool(BM_BufferPool *const bm)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }

    bufferPool *info = (bufferPool*)bm->mgmtData;
    frameNode *currentNode = info->frames->head;

    SM_FileHandle file;

    if (openPageFile ((char *)(bm->pageFile), &file) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }

    //write the block of current node whose dirtyBit is equal to 1 to the file disk;
    while(currentNode != NULL){
        if(currentNode->dirtyBit == 1){
            if(writeBlock(currentNode->pageNum, &file, currentNode->data) != RC_OK){
                return RC_WRITE_FAILED;
            }
            currentNode->dirtyBit = 0;
            (info->numOfWrites)++;
        }
        currentNode = currentNode->next;
    }

    closePageFile(&file);

    return RC_OK;
}

// shut down buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }
    RC status;

    if((status = forceFlushPool(bm)) != RC_OK){
        return status;
    }

    bufferPool *info = (bufferPool *)bm->mgmtData;
    frameNode *currentNode = info->frames->head;

    //    free memory of frame list
    while(currentNode != NULL){
        currentNode = currentNode->next;
        free(info->frames->head->data);
        free(info->frames->head);
        info->frames->head = currentNode;
    }

    info->frames->head = info->frames->tail = NULL;
    free(info->frames);
    free(info);

    bm->numPages = 0;

    return RC_OK;
}


// pin page
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }
    if(pageNum < 0){
        return RC_READ_NON_EXISTING_PAGE;
    }

    if(bm->strategy==RS_FIFO)
        return pinPageUsingFIFOStrategy(bm,page,pageNum);
    else if(bm->strategy==RS_LRU)
        return pinPageUsingLRUStrategy(bm,page,pageNum);
    else if(bm->strategy== RS_LRU_K)
        return pinPageUsingLRU_KStrategy(bm,page,pageNum);
    else
        return RC_UNKNOWN_STRATEGY;


}

//clear fixcount
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }

    bufferPool *info = (bufferPool *)bm->mgmtData;
    frameNode *current;

    // after performing reading/writing operation unpin the page
    if((current = searchNodeByPageNum(info->frames, page->pageNum)) == NULL){
        return RC_NON_EXISTING_PAGE_IN_FRAME;
    }

    // When unpin a page, decrease its fixCount by 1.
    if(current->fixCount > 0){
        current->fixCount--;
    }
    else{
        return RC_NON_EXISTING_PAGE_IN_FRAME;
    }

    return RC_OK;
}

//mark the node someone was using;
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }

    bufferPool *info = (bufferPool *)bm->mgmtData;
    frameNode *current;

    // Find the page if the write operation is performed on it after reading from disk.
    if((current = searchNodeByPageNum(info->frames, page->pageNum)) == NULL){
        return RC_NON_EXISTING_PAGE_IN_FRAME;
    }

    //set the dirty flag of page.
    current->dirtyBit = 1;

    return RC_OK;
}

//write the content of the current node to the file
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)

{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }

    bufferPool *info = (bufferPool *)bm->mgmtData;
    frameNode *current;
    SM_FileHandle file;

    if (openPageFile ((char *)(bm->pageFile), &file) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    // Find the page to be forcefully written back to disk
    if((current = searchNodeByPageNum(info->frames, page->pageNum)) == NULL){
        closePageFile(&file);
        return RC_NON_EXISTING_PAGE_IN_FRAME;

    }
    // Write all the content of page back to disk after identifying it.
    if(writeBlock(current->pageNum, &file, current->data) != RC_OK){
        closePageFile(&file);
        return RC_WRITE_FAILED;
    }

    (info->numOfWrites)++;

    closePageFile(&file);

    return  RC_OK;
}



//return fixcount array;
int *getFixCounts (BM_BufferPool *const bm)
{
    // find the fixCount value of all pages in buffer
    bufferPool *info = (bufferPool *)bm->mgmtData;
    frameNode *cur = info->frames->head;

    while (cur != NULL){
        (info->fixedCounts)[cur->frameNum] = cur->fixCount;
        cur = cur->next;
    }

    return info->fixedCounts;
}

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    // return the frametoPage array
    return ((bufferPool *)bm->mgmtData)->frameToPage;
}
//return the times of write;
int getNumWriteIO (BM_BufferPool *const bm)
{
    // find all those pages which are write after adding to buffer
    return ((bufferPool *)bm->mgmtData)->numOfWrites;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    // find the dirty bit status of all pages in buffer.
    bufferPool *info = (bufferPool *)bm->mgmtData;
    frameNode *cur = info->frames->head;

    while (cur != NULL){
        (info->dirtyFlags)[cur->frameNum] = cur->dirtyBit;
        cur = cur->next;
    }

    return info->dirtyFlags;
}

//return the times of read;
int getNumReadIO (BM_BufferPool *const bm)
{
    // find all those pages which are read after adding to buffer
    return ((bufferPool *)bm->mgmtData)->numOfReads;
}
