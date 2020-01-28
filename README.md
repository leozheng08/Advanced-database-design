# Advanced-database-design
IIT-CS525 coursework

### Starting from a storage manager you will be implementing your own tiny database-like system from scratch. You will explore how to implement the concepts and data structures discussed in the lectures and readings. The assignments will require the use of skills learned in this course as well as other skills you have developed throughout your program. Each assignment will build upon the code developed during the previous assignment. In the end there will be an optional assignment for extra credit. Each of the regular assignments will have optional parts that give extra credit. All assignments have to be implemented using C. We will specify test cases for the assignments, but you are encouraged to add additional test cases. Detailed descriptions will be linked on this page once an assignment is handed out.

* Assignment 1 - Storage Manager: Implement a storage manager that allows read/writing of blocks to/from a file on disk
* Assignment 2 - Buffer Manager: Implement a buffer manager that manages a buffer of blocks in memory including reading/flushing to disk and block replacement (flushing blocks to disk to make space for reading new blocks from disk)
* Assignment 3 - Record Manager: Implement a simple record manager that allows navigation through records, and inserting and deleting records
* Assignment 4 - B+-Tree Index: Implement a disk-based B+-tree index structure

## Assignment 1 - Storage Manager

The goal of this assignment is to implement a simple storage manager - a module that is capable of reading blocks from a file on disk into memory and writing blocks from memory to a file on disk. The storage manager deals with pages (blocks) of fixed size (PAGE_SIZE). In addition to reading and writing pages from a file, it provides methods for creating, opening, and closing files. The storage manager has to maintain several types of information for an open file: The number of total pages in the file, the current page position (for reading and writing), the file name, and a POSIX file descriptor or FILE pointer. In your implementation you should implement the interface described below. Please commit a text file README.txt that (shortly) describes the ideas behind your solution and the code structure. Comment your code!
## Interface
The interface your storage manager should implement is given as a header file storage_mgr.h. The content of this header is shown below. Two additional headers dberror.h and test_helpers.h define error codes and constants and macros used in the test cases.
```c
#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

#include "dberror.h"

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
  char *fileName;
  int totalNumPages;
  int curPagePos;
  void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern void initStorageManager (void);
extern RC createPageFile (char *fileName);
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);
extern RC closePageFile (SM_FileHandle *fHandle);
extern RC destroyPageFile (char *fileName);

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern int getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock (SM_FileHandle *fHandle);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle);

#endif
```
### Data structures
The page size is hard-coded in the header file dberror.h (PAGE_SIZE). Each of the methods defined in the storage manager interface returns an integer return code also defined in dberror.h (RC). For details see return codes below.

The methods in the interface use the following two data structures to store information about files and pages: 
####File Handle SM_FileHandle
A file handle SM_FileHandle represents an open page file. Besides the file name, the handle store the total number of pages in the file and the current page position. The current page position is used by some of the read and write methods of the storage manager. For example, readCurrentBlock reads the curPagePosth page counted from the beginning of the file. When opening a file, the current page should be the first page in the file (curPagePos=0) and the totalNumPages has to be initialized based on the file size. Use the mgmtInfo to store additional information about the file needed by your implementation, e.g., a POSIX file descriptor.

Hint: You should reserve some space in the beginning of a file to store information such as the total number of pages.

Hint: Use mgmtInfo to store any bookkeeping info about a file your storage manager needs.
```c
typedef struct SM_FileHandle {
  char *fileName;
  int totalNumPages;
  int curPagePos;
  void *mgmtInfo;
} SM_FileHandle;
```
####Page Handle SM_PageHandle

A page handle is an pointer to an area in memory storing the data of a page. Methods that write the data pointed to by a page handle to disk or read a page from disk into the area of memory pointed to by the page handle require that the handle is pointing to an previously allocated block of memory that is at least PAGE_SIZE number of bytes long.
```c
typedef char* SM_PageHandle;
```
File Related Methods
createPageFile
Create a new page file fileName. The initial file size should be one page. This method should fill this single page with '\0' bytes.

openPageFile
Opens an existing page file. Should return RC_FILE_NOT_FOUND if the file does not exist. The second parameter is an existing file handle. If opening the file is successful, then the fields of this file handle should be initialized with the information about the opened file. For instance, you would have to read the total number of pages that are stored in the file from disk.

closePageFile, destroyPageFile
Close an open page file or destroy (delete) a page file.

Read and Write Methods
There are two types of read and write methods that have to be implemented: Methods with absolute addressing (e.g., readBlock) and methods that address relative to the current page of a file (e.g., readNextBlock).

readBlock
The method reads the pageNumth block from a file and stores its content in the memory pointed to by the memPage page handle. If the file has less than pageNum pages, the method should return RC_READ_NON_EXISTING_PAGE.

getBlockPos
Return the current page position in a file

readFirstBlock, readLastBlock
Read the first respective last page in a file

readPreviousBlock, readCurrentBlock, readNextBlock
Read the current, previous, or next page relative to the curPagePos of the file. The curPagePos should be moved to the page that was read. If the user tries to read a block before the first page of after the last page of the file, the method should return RC_READ_NON_EXISTING_PAGE.

writeBlock, writeCurrentBlock
Write a page to disk using either the current position or an absolute position.

appendEmptyBlock
Increase the number of pages in the file by one. The new last page should be filled with zero bytes.

ensureCapacity
If the file has less than numberOfPages pages then increase the size to numberOfPages.

Return codes
The header file dberror.h defines several error codes as macros. As you may have noticed the storage manager functions all return an RC value. This value should indicate whether an operation was successful and if not what type of error occurred. If a method call is successful, the function should return RC_OK. The printError function can be used to output an error message based on a return code and the message stored in global variable RC_message (implemented in dberror.c).
## Source Code Structure
* You source code directories should be structured as follows.
* Put all source files in a folder assign1 in your git repository
This folder should contain at least ...
the provided header and C files
a make file for building your code Makefile. This makefile should create a binary from test_assign1 from test_assign1_1.c which requires dberror.c and all your C files implementing the storage_mgr.h interface
a bunch of *.c and *.h files implementing the storage manager
README.txt: A text file that shortly describes your solution
E.g., the structure may look like that:

## Test cases
We have provided a few test case in test_assign1_1.c. You makefile should create an executable test_assign1 from this C file. You are encouraged to write additional tests. Make use of existing debugging and memory checking tools. However, usually at some point you will have to debug an error. See the main assignment page for information about debugging.
