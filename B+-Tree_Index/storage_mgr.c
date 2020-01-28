#include<stdio.h>
#include<stdlib.h>
#include<string.h> 
#include "storage_mgr.h"

void initStorageManager (void) {}

RC createPageFile (char *fileName)
{
	//Open target file and check if it is good.
	FILE* PFile = fopen(fileName, "w");
	if(PFile == NULL) { printf("no file"); return RC_FILE_NOT_FOUND;} 

	SM_PageHandle newPage = (SM_PageHandle)malloc(PAGE_SIZE * sizeof(char));
	memset(newPage, 0, PAGE_SIZE * sizeof(char));//calloc doesn't work wtf.

	if(fwrite(newPage, 1, PAGE_SIZE, PFile) != PAGE_SIZE)
	{
		return RC_WRITE_FAILED;
	}
	fclose(PFile);
	//printf("good write");
	return RC_OK;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle) 
{
	FILE* PFile = fopen(fileName, "r+");
	if(PFile == NULL) { /*printf("bad read!");*/ return RC_FILE_NOT_FOUND;}

	fHandle->fileName = fileName;
	fHandle->curPagePos = 0;

	fseek(PFile, 0L, SEEK_END);
	int temp = ftell(PFile);
	fseek(PFile, 0L, SEEK_SET);
	fHandle->totalNumPages = temp/PAGE_SIZE;
	fHandle->mgmtInfo = PFile;

	fclose(PFile);
	return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle) 
{
	fHandle->mgmtInfo = NULL;
	return RC_OK;
}
RC destroyPageFile (char *fileName) 
{
	FILE* PFile = fopen(fileName, "r");
	if(PFile == NULL) {	return RC_FILE_NOT_FOUND; }
	
	remove(fileName);
	return RC_OK;
}

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//OPen file and check for errors.
	FILE* PFile = fopen(fHandle->fileName, "r");
	if (PFile == NULL) { return RC_FILE_NOT_FOUND; }
	if (pageNum >= fHandle->totalNumPages) { return RC_READ_NON_EXISTING_PAGE; }

	fseek(PFile, (pageNum*PAGE_SIZE), SEEK_SET);
	if(fread(memPage, sizeof(char), PAGE_SIZE, PFile) != PAGE_SIZE) { return RC_ERROR; }

	fHandle->curPagePos = pageNum+1; 
	fclose(PFile);
   	return RC_OK;
}

int getBlockPos (SM_FileHandle *fHandle) { return fHandle->curPagePos; }

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)  { return readBlock(0,fHandle, memPage); }

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) { return readBlock(fHandle->curPagePos-1, fHandle, memPage); }

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) { return readBlock(fHandle->curPagePos, fHandle, memPage); }

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) { return readBlock(fHandle->curPagePos +1, fHandle, memPage); }

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){  return readBlock(fHandle->totalNumPages-1, fHandle, memPage); }

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (pageNum > fHandle->totalNumPages) { return RC_WRITE_FAILED; }
	if (fHandle == NULL) { return RC_WRITE_FAILED; }
	FILE* pageFile = fopen(fHandle->fileName, "w+");
	int index = sizeof(char) * pageNum * PAGE_SIZE;

	//go to intended location, append all memPage characters to the end of our pageFile.
	fseek(pageFile, index, SEEK_SET);
	for(int i = 0; i < PAGE_SIZE; i++) { putc(memPage[i], pageFile); }

	fHandle->curPagePos = ftell(pageFile); 
	fclose(pageFile);//SEGFAULT!!! DO NOT FORGET TO CLOSE.
	return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int pagesize = strlen(memPage);
	if(fHandle == NULL) { return RC_FILE_NOT_FOUND; }
	FILE* PFile = fopen(fHandle->fileName, "w+");

	appendEmptyBlock(fHandle);//gieve space for one more page. go to it, write.
	if (fseek(PFile, fHandle->curPagePos, SEEK_SET) != 0) { return RC_WRITE_FAILED; }
	fwrite(memPage, sizeof(char), pagesize, PFile);
	
	fHandle->curPagePos = ftell(PFile);
	fclose(PFile);//SEGFAULT!!! DO NOT FORGET TO CLOSE.
	//free(newPage);
	return RC_OK;
}

RC appendEmptyBlock (SM_FileHandle *fHandle) 
{
	FILE* PFile = fopen(fHandle->fileName, "r+");
	SM_PageHandle newPage = (SM_PageHandle)malloc(PAGE_SIZE * sizeof(char));
	memset(newPage, 0, PAGE_SIZE);

	if (fseek(PFile, 0, SEEK_END) != 0) { return RC_WRITE_FAILED; }
	fHandle->totalNumPages += 1;
	fwrite(newPage, 1, PAGE_SIZE, PFile);
	return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	if (fHandle == NULL) { return RC_FILE_NOT_FOUND; }
	
	int looper = numberOfPages - fHandle->totalNumPages;

	for(int i = looper; i > 0; i--)
	{
		appendEmptyBlock(fHandle);
	}
	return RC_OK;
}
