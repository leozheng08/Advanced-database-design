#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "storage_mgr.h"



extern void initStorageManager (void) {
    // Initialising file pointer i.e. storage manager.
    printf("<-------------Initiate Storage Manager--------------->");
    printf("Team member: Zeyuan Zheng CWID: A20432839");
}

extern RC createPageFile (char *fileName) {
    // Opening file stream in read & write mode. 'w+' mode creates an empty file for both reading and writing.
    FILE *file = fopen(fileName, "w+");
    if(file!=NULL){
        SM_PageHandle memoryBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
        memset(memoryBlock,'\0',PAGE_SIZE);
        fwrite(memoryBlock, sizeof(char), PAGE_SIZE,file);
        fclose(file);
        free(memoryBlock);
        return RC_OK;
    }
    else{
        return RC_FILE_NOT_FOUND;
    }
}

extern RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
    FILE *file = fopen(fileName, "r+");

    if(file != NULL) {
        fHandle->fileName = fileName;
        fHandle->curPagePos = 0;

        fseek(file,0,SEEK_END);
        int i = (int) ftell(file);
        int j = i/ PAGE_SIZE;
        fHandle->totalNumPages = j;
        fHandle->mgmtInfo =file;
        return RC_OK;
    } else {
        return RC_FILE_NOT_FOUND;
    }
}

extern RC closePageFile (SM_FileHandle *fHandle) {
    if (fHandle->mgmtInfo != NULL)
    {
        if(!fclose(fHandle->mgmtInfo))
        {
            return RC_OK;
        }
        else{
            return RC_FILE_NOT_FOUND;
        }
    }
    else{

        return RC_FILE_HANDLE_NOT_INIT;
    }
}


extern RC destroyPageFile (char *fileName) {
    if(fopen(fileName, "r") == NULL){
        return RC_FILE_NOT_FOUND;
    }
    remove(fileName);
    return RC_OK;
}

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    FILE *file = fHandle->mgmtInfo;
    if (fHandle == NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }
    if(file == NULL){
        return RC_FILE_NOT_FOUND;
    }
    if( fHandle->totalNumPages<pageNum || pageNum < 0){
        return RC_READ_NON_EXISTING_PAGE;
    }
    fseek(file, (pageNum+1)*PAGE_SIZE*sizeof(char), SEEK_SET);
    fread(memPage, 1, PAGE_SIZE, file);
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

extern int getBlockPos (SM_FileHandle *fHandle) {
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    else{
        if (fopen(fHandle->fileName, "r")==NULL)
        {
            return RC_FILE_NOT_FOUND;
        }
        return fHandle->curPagePos;

    }
}

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return  readBlock(0,fHandle,memPage);
}

extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos-1,fHandle,memPage);
}

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return  readBlock(fHandle->curPagePos,fHandle,memPage);
}

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return  readBlock(fHandle->curPagePos+1,fHandle,memPage);
}

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return  readBlock(fHandle->totalNumPages-1,fHandle,memPage);
}

extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if(fHandle==NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }
    FILE *file = fHandle->mgmtInfo;
    if(file == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }
    int position = (pageNum+1)*PAGE_SIZE;
    if((pageNum+1)!=0){
        fHandle->curPagePos = position;
        writeCurrentBlock(fHandle,memPage);
    }
    else{
        fseek(file,position,SEEK_SET);
        for(int i=0; i<PAGE_SIZE; i++)
        {
            if(feof(file)){
                appendEmptyBlock(fHandle);
            }
            fputc(memPage[i],file);
        }
        fHandle->curPagePos = ftell(file)/PAGE_SIZE;
        fclose(file);
    }
    return RC_OK;
}
//
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if(fHandle==NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }
    FILE *file = fopen(fHandle->fileName, "r+");
    if(file == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }
    long int Position = fHandle->curPagePos;
    fseek(file,Position,SEEK_SET);
    fwrite(memPage, sizeof(char),PAGE_SIZE,file);
    fHandle->curPagePos = ftell(file);
    fclose(file);
    return RC_OK;

}
//
//
extern RC appendEmptyBlock (SM_FileHandle *fHandle) {
    if(fHandle==NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }
    SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE,sizeof(char));
    fseek(fHandle->mgmtInfo, (fHandle->totalNumPages + 1)*PAGE_SIZE*sizeof(char), SEEK_END);
    fwrite(emptyPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo);
    free(emptyPage);
    fHandle->totalNumPages++;
    return RC_OK;
}
//
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
    if(fHandle==NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }
    while(numberOfPages > fHandle->totalNumPages){
        appendEmptyBlock(fHandle);
    }
    return RC_OK;
}
