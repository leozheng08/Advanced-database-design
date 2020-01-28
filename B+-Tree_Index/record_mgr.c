#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <ctype.h>

int pagesScanned = 0;
RecordManager* MasterRM;


RC initRecordManager(void *mgmtData) { return RC_OK; }

RC shutdownRecordManager () { return RC_OK; }

//Procedure:
//Split into two different parts
//1. initialize and pre-set all masterRM required information.
//2. Create a tombpage, and save if to disk.
//
RC createTable (char *name, Schema *schema)
{
	MasterRM = malloc(sizeof(RecordManager));
	initBufferPool(&MasterRM->buffer, name, 5000, RS_FIFO, NULL);
	MasterRM->availablePage = 1;
	MasterRM->recordCount = 0;
	MasterRM->snumAttr = schema->numAttr;
	//NOTE: reduce confusion:
 	//Rather than go through memory and counting bits
 	//Add schema attribute related fields to the RecordManager.
	for(int i = 0; i < schema->numAttr; i++) { strcpy(&MasterRM->sattrName[i],schema->attrNames[i]); }
	for(int i = 0; i < schema->numAttr; i++) { MasterRM->stypeLength[i] = schema->typeLength[i]; }
	for(int i = 0; i < schema->numAttr; i++) { MasterRM->sdataTypes[i] = schema->dataTypes[i]; }

	//Creating a table should create the underlying page file and store information about the shcema, free space
	//and so on in the Table Informatino Pages.
	/*	Page Layout:
		(4 bits for page number) -> (4 bits for record count) -> (4 bits for attributes/record) -> (4 bits reserve)->
		-> (record attributes)

		record attributes:
		Storage	sequence ---row by row, ie all attribute names, then all data types, then all datatype sizes--->
								record1		record2		record3		record4		....
				 |				attrName1	attrName2	attrName3	attrName4
				 |				dataType1	dataType2	dataType3	dataType4
				\|/				typeSize1	typeSize2	typeSize3	typeSize4
				 `				.
								.
		Basically, store all attribute names... then all data types... then all typelengths.... in sequence.
		Layout: attribute by attribute, not record by record...
		Note that, EACH attribute entry (name, type, length) is separated by a tombstone character. May not be necessary
	*/

	char* newPage = buildTomb(PAGE_SIZE);	//tombstone string.
	char* pt = newPage;

	*pt = '1'; pt += sizeof(int);	//Page number begins with "1"
	*pt = '0'; pt += sizeof(int);	//Number of records in this page is 0
	*pt = schema->numAttr; pt += sizeof(int);	//Number of attributes per record.
	pt += sizeof(int);	//Reserve another spot for future modifications.
 	for(int i = 0; i < schema->numAttr; i++)
 	{
 		strcpy(pt, schema->attrNames[i]);
 		pt+=strlen(schema->attrNames[i]); *pt = '\0'; pt+1;
 		//Note that attrName is a string, need a legit EOF character, and not our tombstone char.
 	}
 	for(int i = 0; i < schema->numAttr; i++) { *pt = schema->dataTypes[i]; pt += sizeof(DataType); }
 	for(int i = 0; i < schema->numAttr; i++) { *pt = schema->typeLength[i]; pt += sizeof(int); }

	int peep = 50;
//	for(int i = 0; i < peep; i++) { printf(" --DataPeeper---------    %d   %c\n", data[i], data[i]); }
	
	int checker = 0;
	SM_FileHandle result;
	checker += createPageFile(name);
	checker += openPageFile(name, &result);
	checker += writeBlock(0, &result, newPage);
	checker += closePageFile(&result);
 	if(checker != 0) { return RC_ERROR_TRACER_ZERO; }

	return RC_OK;
}


//All operations on a table such as scanning or inserting recrods require the table to be opened first.
//NO disk access necessary, all necessary data are loaded into our masterRM when page was created.
RC openTable (RM_TableData *rel, char *name)
{ 
	rel->name = name;
	rel->mgmtData = MasterRM; //data is all stored in the master RM.
	Schema* sch = malloc(sizeof(Schema));
	int looper = 0;
	sch->numAttr = looper = MasterRM->snumAttr;
	//MasterRM->availablePage = sch->apage;

	//For each attribute metta data, allocate enough memory.
	//Then, for each occurence of an attribute, set it accordinly.
	sch->attrNames = malloc(sch->numAttr * sizeof(char *)); for(int i = 0; i < looper; i++) { sch->attrNames[i] = &MasterRM->sattrName[i]; }
	sch->dataTypes = malloc(sch->numAttr * sizeof(DataType)); for(int i = 0; i < looper; i++) { sch->dataTypes[i] = MasterRM->sdataTypes[i]; }
	sch->typeLength = malloc(sch->numAttr * sizeof(int)); for(int i = 0; i < looper; i++) { sch->typeLength[i] = MasterRM->stypeLength[i]; }
 
	//NOTE: Potential danger:
	//When I dereference the attribute name string pointer, and set it to  a string value... segmentation fault...
	//SO! very dangerously, I just made it point to wherever the source data is... no segmentation fault... but data integrity coule be compromised.
	int peep = 1;
	//for(int i = 0; i < peep; i++) { printf(" --DataPeeper-------%d    %d   %d\n", MasterRM->availablePage, MasterRM->recordCount, MasterRM->snumAttr); }
	rel->schema = sch;
	return RC_OK;
}


//"Closing a table should cause all outstanding changes to the table to be written ot the page file."
RC closeTable (RM_TableData *rel)
{
	RecordManager* bufferholder = rel->mgmtData;
	forceFlushPool(&bufferholder->buffer);
	return RC_OK;
}


RC deleteTable (char *name) { return RC_OK; }


int getNumTuples (RM_TableData *rel) { return ((RecordManager*)rel->mgmtData)->recordCount; }


//Get intombed slot number, logic:
//Loop through the entire page in chunks of size recordSize.
//Lopp trhough the chunks, if all characters in a particular chunk are #,
//THen that chunk of data is the intombed record.
int getTombStoneSlotNumber(char* data, int recordSize)
{
	int returnValue = -1;
	for(int i = 0; i < PAGE_SIZE/recordSize; i+=recordSize)
	{
		bool bingo = true;

		for(int j = 0; j < recordSize; j++)
		{
			if (data[j+i] != '#') { bingo = false; break; }	
		}
		if(bingo == true ){	return i/recordSize; }
	}
	return returnValue;
}


//New proposal:
//In the masterRM, record page number of an available page. Deleting a record will render a page available.
//If a page's only tombstone slot has been filled, it is no longer an available page, update available page to the NEXT page.
//1. Pin an available page, find its free slot.
//2. If there are no free slots, attach it at the end, if overflow, then move to the next page and insert data at the 0th slot.
//3. AFter determining correct page+slot position, copy data over. then commit changes to disk.
RC insertRecord (RM_TableData *rel, Record *record)
{
	bool overflow = false;
	RecordManager* RManager = rel->mgmtData; 
	int recordSize = getRecordSize(rel->schema);
	int graveyardPage = RManager->availablePage;	//page with the tombstone slot.

	//1. Pin the presumed available page, look at its data. Find a free slot if any.
	pinPage(&RManager->buffer, &RManager->page, graveyardPage);

	//let k be location of free slot.
	int k = getTombStoneSlotNumber(RManager->page.data, recordSize);
	if(k < 0) { k = RManager->recordCount; } //if no free slot, put record as the last one in table.
	//If the kth record is predicted to be outside the current page, cue overflow.
	if(k*recordSize > PAGE_SIZE) { overflow = true; }	//Check for potential overflow

	//printf("bbbbbbbbbbb        %d        %d           \n ", RManager->recordCount > PAGE_SIZE/recordSize, (k == -1));

	//2. If overflow, move to the next page and store the overflowed record as the first in the page.
	if(overflow)
	{
		//Shift focus to next page:
		unpinPage(&RManager->buffer,&RManager->page);
		graveyardPage++;
		pinPage(&RManager->buffer,&RManager->page, graveyardPage);
		//put record at the first slot on the next page.
		k = 0;
	}
	record->id.page = graveyardPage;
	record->id.slot = k;
	//CHECKPOINT: record ID is now correctly set.
	//printf("aaaaaaa     %d             %d \n", record->id.page, record->id.slot);

	//3. The page and slot number is now known. use it to determine an address.
	//   THen copy the data over, and commit changes to disk.
	char* pageData = RManager->page.data; 	
	for(int i = 0, offset = k*recordSize; i < recordSize; i++, offset++)
	{
		pageData[offset] = record->data[i];
	}

	//COmmit changes to disk.
	RManager->recordCount = k+1;
	RManager->availablePage = graveyardPage;
	return commitToDisk(&RManager->buffer,&RManager->page);
}


//Procedure:
//Load the given record into a buffer for processing
//overwrite the destination data with our source.
//Commit changes to disk.
RC updateRecord (RM_TableData *rel, Record *record)
{	
	//Load page into buffer.
	RecordManager* r = rel->mgmtData;
	BM_BufferPool tempBuffer = r->buffer;
	BM_PageHandle tempBPageH = r->page;
	pinPage(&tempBuffer, &tempBPageH, record->id.page);
	//printf("aaaaaaaaaaaaaaa"); 
	//calculate data position and overwrite target with source
	int dataPosition = record->id.slot * getRecordSize(rel->schema);
	char* temp = tempBPageH.data + dataPosition;
	strncpy(temp, record->data, getRecordSize(rel->schema));

	//COmmit changes ot disk.
	return commitToDisk(&tempBuffer, &tempBPageH);	
}


//Given how long a record should be, create a free (intombed) record of the designated length.
char* buildTomb(int recordLength)
{
	char* returnValue = (char*)malloc(recordLength);
	for( int i = 0; i < recordLength; i ++)
	{
		returnValue[i] = '#';
	}
	//returnValue[recordLength] = '\0';
	return returnValue;
}


//Procedure:
//Load page with given ID from table into a buffer.
//Find the slot of the given id, intomb it.
RC deleteRecord(RM_TableData *rel, RID id){
	RecordManager* r = rel->mgmtData;
	BM_BufferPool tempBuffer = r->buffer;
	BM_PageHandle tempBMPage = r->page;
	pinPage(&tempBuffer, &tempBMPage, id.page);
	r->availablePage = id.page;

	char* killData = tempBMPage.data;
	killData += id.slot * getRecordSize(rel->schema);
	int endOfDelete = killData + getRecordSize(rel->schema);

	for(int i = killData; i < endOfDelete; i ++) { *killData = '#'; }
	
	return commitToDisk(&tempBuffer,&tempBMPage);
}


//Procedure:
//Load page containing given rid into buffer, then access its data.
//locate the record specified in the id.
//copy the record data over into the record.
RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	RecordManager* r = rel->mgmtData;
	BM_BufferPool tempBuffer = r->buffer;
	BM_PageHandle tempBPageH = r->page;
	
	if( pinPage(&tempBuffer, &tempBPageH, id.page) != RC_OK ) { return RC_ERROR_TRACER_ONE; }
	char* dataPosition = tempBPageH.data + (id.slot * getRecordSize(rel->schema));
	//bool isDead = isDataDead(dataPosition, PAGE_SIZE);
	
	record->id = id;
	char* data = record->data;
	//?#$!@#%$!#@ DO NOT USE = FOR POINTERS!USE MEMCPY
	strncpy(data, dataPosition, getRecordSize(rel->schema));

	if( unpinPage(&tempBuffer, &tempBPageH) != RC_OK ) { return RC_ERROR_TRACER_ONE; }

	//if(isDead) { printf("target retrieved is tombstone"); }
	return RC_OK;
}


//Debugger.
bool isDataDead(char* dataStartPosition, int dataLength)
{
	bool returnValue = TRUE;
	for(int i = 0; i < dataLength; i ++)
	{
		if(dataStartPosition[i] != '#') { return false; }
	}
	return returnValue;
}


//Starting a scan initializes rmscnhandle.
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	scan->rel = rel;
	scan->mgmtData = malloc(sizeof(Expr));
	scan->mgmtData = cond;
	//printf("aaaaaaaaaaaaaaa"); 
	return RC_OK;
}


//Procedure:
//Go through all UNSCANNED pages, record by record, compare data using given expression find a match.
//Make note of which page the current expression has been fulfilled, so that the next time NEXT is called,
//This.page is skipped, and scan begins AFTER this.page.
RC next (RM_ScanHandle *scan, Record *record)
{ 
	RecordManager* scanner = (RecordManager*)scan->rel->mgmtData;
	BM_BufferPool* tempBuffer = & scanner->buffer;
	Expr* cond = (Expr*)scan->mgmtData; 
	Value* comparer = (Value *) malloc(sizeof(Value));
	int recordsInTable = scanner->recordCount;
	int recordSize = getRecordSize(scan->rel->schema);
	int pagesInDisk = tempBuffer->BMFH->totalNumPages;

	//Go through all pages in disk.
	for(int p = 1; p <= pagesInDisk; p++)
	{
		//LOok into the pages recordbyrecord, and compare with what's given.
		pinPage(&MasterRM->buffer, &MasterRM->page, p);
		for(int s = pagesScanned; s <= recordsInTable; s++)
		{
			pagesScanned++;
			char* targetLocation = scanner->page.data + (recordSize) * s; //This is where the target data is.
 			char* sourceLocation = record->data;
 			//set the page, and slot values accordingly.
			record->id.page = p;
			record->id.slot = s;
			memcpy(sourceLocation,targetLocation,recordSize);
			evalExpr(record, scan->rel->schema, cond, &comparer);

			//If a match is found, clean up. return immediately.
			if(comparer->v.boolV == TRUE) { unpinPage(&MasterRM->buffer, &MasterRM->page); return RC_OK; }
		}
		//If no match is found, then we move to next page(shift pin status to next page).
		unpinPage(&MasterRM->buffer,&MasterRM->page);
		if(pagesScanned>recordsInTable) {pagesScanned = 0;}
	}
	return RC_RM_NO_MORE_TUPLES;
}


//Closing a scan indicates ot the recordmanager that all associated resources can be cleaned up.
RC closeScan (RM_ScanHandle *scan)
{
	//free(scan->mgmtData);//segfault? leave it?
	return RC_OK;
}


//Go into the given schema, sum up size of all atriutes.
int getRecordSize (Schema *schema)
{
	int returnValue = 0;
	for(int i = 0; i < schema->numAttr; i++)	//go into schema
	{
		if(schema->dataTypes[i] == DT_INT) { returnValue += sizeof(int); continue; }
		if(schema->dataTypes[i] == DT_FLOAT) { returnValue += sizeof(float); continue; }
		if(schema->dataTypes[i] == DT_BOOL) { returnValue += sizeof(bool); continue; }
		if(schema->dataTypes[i] == DT_STRING) { returnValue += schema->typeLength[i]; continue; }
	}
	if(returnValue == 0) { return RC_ERROR; }
	return returnValue;
}


//Given all schema attributes, create a new one.
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	Schema* returnValue = (Schema*)malloc(sizeof(Schema));
	returnValue->numAttr = numAttr;
	returnValue->attrNames = attrNames;
	returnValue->dataTypes = dataTypes;
	returnValue->typeLength = typeLength;
	returnValue->keySize = keySize;
	returnValue->keyAttrs = keys;

	return returnValue;
}


//Free schema?
RC freeSchema (Schema *schema)
{
	free(schema);
	return RC_OK;
}


//Create and initialize a new record.
//Record assumed to be marked with tombstone, and NOT in any page nor slot.
RC createRecord (Record **record, Schema *schema)
{
	Record* result = (Record*)malloc(sizeof(Record));
	result->id.page = -1;
	result->id.slot = -1;
	//create new record, and fill with tombstone, indicate that's empty.
	result->data = buildTomb(getRecordSize(schema));
	*record = result;
	return RC_OK;
}
 

RC freeRecord (Record *record)
{
	free(record);
	return RC_OK;
}


//Procedure:
//1. Find where the attribute is should be (location of attrNum-th attribute)
//2. Determine what value does it hold.
//3. Set the data accordingly.
//Int, string, float, bool.
//for integers, break it down to its individual digits, then store them individually.
//Exact same for float.
//copy over string values.
//store bool value as a single bit, 1 or 0.
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{ 
	//1. Find where the attribute is should be (location of attrNum-th attribute head)
	int attributeLoc = 0;
	for(int i = 0; i < attrNum; i++)
	{
		if(schema->dataTypes[i] == DT_INT) { attributeLoc += sizeof(int); }
		if(schema->dataTypes[i] == DT_STRING) { attributeLoc += schema->typeLength[i]; }
		if(schema->dataTypes[i] == DT_FLOAT) { attributeLoc += sizeof(float); }
		if(schema->dataTypes[i] == DT_BOOL) { attributeLoc += sizeof(bool); }
	}

	//2. Determine what value does the given attribute hold.
	DataType attributeType = schema->dataTypes[attrNum]; 

	//3. Set the data accordingly.
	if(attributeType == DT_INT)
	{
		//store integer as string of digits...
		//1. Int to String.
		int digits = snprintf(NULL, 0, "%d", value->v.intV); //MUst have 1+? or one digit short? huh?
		char* intString = malloc(digits+1); //need one more for '\0' char of string?
		snprintf(intString, digits+1, "%d", value->v.intV);
		//printf("the intege is %d\nthe string is %s\n", value->v.intV, intString);

		//2. Save each digit individually.
		for(int i = 0; i < digits; i++) 
		{
			//printf("the digits are %c\n", intString[i]);
			record->data[attributeLoc + i] = intString[i];
		}
		return RC_OK;
	}

	if(attributeType == DT_STRING)
	{
		char* recordLocation = record->data + attributeLoc;
		memcpy(recordLocation, value->v.stringV, schema->typeLength[attrNum]);
		return RC_OK;
	}
	
	if(attributeType == DT_FLOAT)	//same as int.
	{
		int digits = snprintf(NULL, 0, "%f", value->v.floatV); //MUst have 1+? or one digit short? huh?
		char* floatString = malloc(digits+1); //need one more for '\0' char of string?
		snprintf(floatString, digits+1, "%f", value->v.floatV);
		//printf("the intege is %d\nthe string is %s\n", value->v.intV, floatString);

		//2. Save each digit individually.
		for(int i = 0; i < digits; i++) 
		{
			//printf("the digits are %c\n", floatString[i]);
			record->data[attributeLoc + i] = floatString[i];
		}
		return RC_OK;
	}
	
	if (attributeType == DT_BOOL)	//boolean is a single digit, 1 for true, 0 for false.
	{
		char* result = malloc(sizeof(char));/////just a single size char pointer.
		if(value->v.boolV) { *result = '1'; }
		else { *result = '0'; }
		record->data[attributeLoc] = *result;
		return RC_OK;
	} 
	return RC_ERROR;
}


//Procedure:
//1. Find where the attribute is should be (location of attrNum-th attribute)
//2. Determine what value does it hold.
//3. Set the data accordingly.
//Essentially the same as settAttr but reversed.
//Int, string, float, bool.
//for integers, read its digits individually, then patch them togther.
//for floats, just like ints, but allow ONE decimal point.
//copy over string values.
//for bools, the value is 1 or 0, read it, interpret it..
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	//1. Find where the attribute is should be (location of attrNum-th attribute head)
	Value* result = malloc(sizeof(Value));

	int attributeLoc = 0;
	for(int i = 0; i < attrNum; i++)
	{
		if(schema->dataTypes[i] == DT_INT) { attributeLoc += sizeof(int); }
		if(schema->dataTypes[i] == DT_FLOAT) { attributeLoc += sizeof(float); }
		if(schema->dataTypes[i] == DT_BOOL) { attributeLoc += sizeof(bool); }
		if(schema->dataTypes[i] == DT_STRING) { attributeLoc += schema->typeLength[i]; }
	}

	//2. Determine what value does the source attribute hold.
	DataType attributeType = schema->dataTypes[attrNum];

	//3. Find the data at in the give record, reverse whatever is done in setAttr.	
	if(attributeType == DT_INT)	//WTF isdigit doesn't work, atoi doesn't work....
	{
		char* dataLocation = record->data + attributeLoc;
		char* intString = malloc(PAGE_SIZE);

		int viewer = PAGE_SIZE;
		//for(int k = 0; k < viewer; k++) { printf("data at location is %c\n",dataLocation[k]); } //raw data viewer

		//if the character at datalocation is a digit, concatinate it to intString.
		int digitsCount = 0;
		for(int i = 0; i < PAGE_SIZE; i ++)
		{
			if(dataLocation[i] >= '0' && dataLocation[i] <= '9') { digitsCount++; }
			else { break; }
		}
		//BREAKPOINT: now the digits of the integer begins at datalocation and is digitsCounts long.

		//Take the substring of digits, conver to integer
		strncpy(intString, dataLocation, digitsCount);

		//char *a = "3333089";
		//printf("mmmmmmmmmmmmmmmmmmmmm %d \n", atoi(a));
		result->v.intV = trueATOI(intString, digitsCount);
	 	//printf("aaaaaaaaaaaaaaaaaaaaaaaa %d \n", result->v.intV);

		result->dt = 0;	
	}

	if(attributeType == DT_STRING) //Patch an end of string character at the end.
	{
		int stringSize = schema->typeLength[attrNum];
		char* dataLocation = record->data + attributeLoc;
		result->v.stringV = malloc(stringSize+1);

		strncpy(result->v.stringV, dataLocation, stringSize);
		result->v.stringV[stringSize] = '\0';
		result->dt = 1;
	}

	if(attributeType == DT_FLOAT)	//same as int, but this time, allow one decimal point.
	{

		char* dataLocation = record->data + attributeLoc;
		char* intString = malloc(PAGE_SIZE);

		//if the character at datalocation is a digit, concatinate it to intString.
		int digitsCount = 0;
		bool decimalPoint = false;
		for(int i = 0; i < PAGE_SIZE; i ++)	//basically: bring in all contiguous digit bits and ONE decimal bit.
		{
			if(dataLocation[i] >= '0' && dataLocation[i] <= '9' ||  (dataLocation[i] == '.' && decimalPoint == false))
			{
				digitsCount++;
				if(dataLocation[i] == '.') { decimalPoint = true; }
			}
			else { break; }
		}
		//BREAKPOINT: now the string contains the full float value.

		//Take the substring of digits, conver to float
		strncpy(intString, dataLocation, digitsCount);

		result->v.floatV = strtod(intString, NULL);
		result->dt = 2;
	}

	if(attributeType == DT_BOOL)
	{			
		char* dataLocation = record->data + attributeLoc;
		if(dataLocation[attributeLoc] == '1') { result->v.boolV = true; }
		else { result->v.boolV = false; }
		result->dt = 3;
	}

	*value = result;
	return RC_OK;
}


int trueATOI(char* c, int length)
{
	int result = 0;
	for(int i = 0; i < length; i ++)
	{
		result *= 10;
		result += (c[i] - '0');
	}
	return result;
}

RC commitToDisk(BM_BufferPool * bm, BM_PageHandle* bp)
{
	int checker = 0;
	checker += unpinPage(bm, bp);
	checker += forcePage(bm, bp);
	if (checker == 0) { return RC_OK; }
	return RC_ERROR_TRACER_TWO;
}