#include <stdio.h>
#include "storage_mgr.h"
#include "dberror.h"
#include <stdlib.h>
#include <string.h>

FILE *file;

void initStorageManager (void){
    printf("-----------------initiate storage manager------------------\n");
    printf("team worker: Zeyuna Zheng CWID:A20432839\n");
}

//the function of creating a page file
RC createPageFile (char *fileName){
    RC result;
//    open a file called fileName in write+ mode;
    file = fopen(fileName,"w+");
    if(file!=NULL){
//      initiate a memory block which has a PAGE_SIZE;
        char *memBlock = calloc(PAGE_SIZE,sizeof(char));
//        inset PAGE_SIZE '\0' into memBlock;
        memset(memBlock,'\0',PAGE_SIZE);
//        write the memory block into file;
        fwrite(memBlock,sizeof(char),PAGE_SIZE,file);
//        free memory block;
        free(memBlock);
//        close file;
        fclose(file);
        file=NULL;
        result = RC_OK;
    }
    else{
        result = RC_FILE_NOT_FOUND;
    }
    return result;
}

//the function of open a page file
RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    RC result;
    //    open a file called fileName in read+ mode;
    file = fopen(fileName,"r+");
    if(file==NULL){
        result = RC_FILE_NOT_FOUND;
    }
    else{
//        set the file pointer to the end of the file;
        fseek(file,0,SEEK_END);
//        return the length of from  initial location to the last location;
        int len = ftell(file);
//        return the number of page;
        int totalPages = len/PAGE_SIZE;
//        set the file name;
        (*fHandle).fileName = fileName;
//        set the page number;
        (*fHandle).totalNumPages = totalPages;
//        set the current page location;
        (*fHandle).curPagePos = 0;
//        set the location of current operating file
        (*fHandle).mgmtInfo = file;
        rewind(file);
        result = RC_OK;
    }
    return result;
}

//the function of close page file
// use (*fHandle).mgmtInfo instead of file
RC closePageFile (SM_FileHandle *fHandle){
    int isfileClosed;
    RC result;
    isfileClosed = fclose((*fHandle).mgmtInfo);
//    if fclose return 0, the operation is successful;
    if(isfileClosed==0){
        (*fHandle).mgmtInfo=NULL;
        result = RC_OK;
    }
    else{
        result = RC_FILE_NOT_FOUND;
    }
    return result;


}

//kill the page file
RC destroyPageFile (char *fileName){
    int isFileDestoried;
    RC result;
    isFileDestoried =remove(fileName);
//    if remove function return 0, the operation is successful;
    if(isFileDestoried==0){
        fileName=NULL;
        result = RC_OK;
    }
    else{
        result = RC_FILE_NOT_FOUND;
    }
    return result;
}

// the function of reading pageNum^th block
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC result;
//    judge the condition of current operating file;
    if(fHandle==NULL){
        result = RC_FILE_HANDLE_NOT_INIT;
    }
    else{
//        judge the total number of page does not exceed reading page;
        if((*fHandle).totalNumPages<=pageNum||pageNum<0){
        result = RC_READ_NON_EXISTING_PAGE;
        }
        else{
//            set the file pointer to the reading page position;
            fseek((*fHandle).mgmtInfo,PAGE_SIZE*pageNum,SEEK_SET);
//            read the whole page of current page position;
            RC readSize = fread(memPage,sizeof(char),PAGE_SIZE,(*fHandle).mgmtInfo);
//            if fread function returns the value which is equal toPAGE_SIZE, function works successfully;
            if(readSize==PAGE_SIZE){
                (*fHandle).curPagePos=pageNum;
                result = RC_OK;
                rewind((*fHandle).mgmtInfo);
            }
            else{
                result = RC_READ_NON_EXISTING_PAGE;
            }
        
        }
    }
    return result;
}

// the function of return current page position;
int getBlockPos (SM_FileHandle *fHandle){
    if(fHandle==NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }
    int BlockPos;
    BlockPos = (*fHandle).curPagePos;
    return BlockPos;
}

//the function of reading first page block;
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(0,fHandle,memPage);
}

//the function of reading previous page block;
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock((*fHandle).curPagePos-1,fHandle,memPage);
}

//the function of reading current page block;
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock((*fHandle).curPagePos,fHandle,memPage);
}
//the function of reading next page block
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock((*fHandle).curPagePos+1,fHandle,memPage);
}
//the function of reading last page block;
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock((*fHandle).totalNumPages-1,fHandle,memPage);
}

// thd function of writing pageNum^th page block;
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC result;

    if(fHandle==NULL){
        result = RC_FILE_HANDLE_NOT_INIT; 
    }
    else{

        if((*fHandle).totalNumPages<=pageNum||pageNum<0){
            result = RC_WRITE_FAILED;
        }
        else{
            fseek((*fHandle).mgmtInfo,PAGE_SIZE*pageNum,SEEK_SET);
            int writeSize=fwrite(memPage,sizeof(char),PAGE_SIZE,(*fHandle).mgmtInfo);
//            if the value fwrite returns is equal to PAGE_SIZE, the function works successfully;
            if(writeSize==PAGE_SIZE){
                (*fHandle).curPagePos=pageNum;
                fseek((*fHandle).mgmtInfo,0,SEEK_END);
                (*fHandle).totalNumPages = ftell((*fHandle).mgmtInfo)/PAGE_SIZE;
//                return the file pointer to the initial position of page file
                rewind((*fHandle).mgmtInfo);
                result = RC_OK;
            }
            else{
                result=RC_WRITE_FAILED;
            }
        }
    }

    return result;
}

// the function of writing the current page block;
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return writeBlock((*fHandle).curPagePos,fHandle,memPage);
}

//the function of writing a page block to the last position of file
RC appendEmptyBlock (SM_FileHandle *fHandle){
    RC result;
    int size = 0;
    if(file==NULL){
//        set a PAGE_SIZE empty memory block;
        char *EmptyMemBlock = calloc(PAGE_SIZE,sizeof(char));
//        set the file pointer to the end of page file;
        fseek((*fHandle).mgmtInfo,0,SEEK_END);
//        write a memory block to the file pointer position;
        size = fwrite(EmptyMemBlock,sizeof(char),PAGE_SIZE,(*fHandle).mgmtInfo);
        if(size==PAGE_SIZE){
            (*fHandle).totalNumPages = (*fHandle).totalNumPages+1;
            (*fHandle).curPagePos = (*fHandle).totalNumPages-1;
            result = RC_OK;
        }
        else{
            result = RC_WRITE_FAILED;
        }
        free(EmptyMemBlock);
        EmptyMemBlock=NULL;
    }
    else{
        result = RC_FILE_NOT_FOUND;
    }
    
    return result;
}

//the function of ensure the page number of file is numberOfPages, if not, append empty blocks
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    RC result;
    int totalPages = (*fHandle).totalNumPages;
    if(numberOfPages>totalPages){
        int AddPages = numberOfPages-totalPages;
        for(int i=0;i<AddPages;i++){
            appendEmptyBlock(fHandle);
        }
        result = RC_OK;
        
    }
}


