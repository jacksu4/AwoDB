
#ifndef TABLE_RW_C
#define TABLE_RW_C

#include <fstream>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"

using namespace std;

MyDB_TableReaderWriter :: MyDB_TableReaderWriter (MyDB_TablePtr forme, MyDB_BufferManagerPtr myBuffer) {
	myTable = forme;
	myBufferManager = myBuffer;	
	if(myTable->lastPage() == -1) {
		myTable->setLastPage(0);
		myBufferManager->getPage(myTable, 0);
	}
}

MyDB_PageReaderWriter MyDB_TableReaderWriter :: operator [] (size_t i) {
	while(myTable->lastPage() < i) {
		myTable->setLastPage(myTable->getLast() + 1);
		myBufferManager->getPage(myTable, myTable->getLast());
	}
	curPageManager = make_shared<MyDB_PageReaderWriter> (make_shared <MyDB_PageHandleBase> (i), myBufferManager->getPageSize());
	return *curPageManager;
}

MyDB_RecordPtr MyDB_TableReaderWriter :: getEmptyRecord () {
	return make_shared <MyDB_Record> (myTable->getSchema());
}

MyDB_PageReaderWriter MyDB_TableReaderWriter :: last () {
	curPageManager = make_shared<MyDB_PageReaderWriter> (make_shared <MyDB_PageHandleBase> (myTable->lastPage()), myBufferManager->getPageSize());
	return *curPageManager; 
}


void MyDB_TableReaderWriter :: append (MyDB_RecordPtr appendMe) {
	// try to append the record on the current page...
	MyDB_PageReaderWriterPtr lastPage = 
	make_shared<MyDB_PageReaderWriter> (make_shared <MyDB_PageHandleBase> (myTable->lastPage()), myBufferManager->getPageSize());
	if (!lastPage->append (appendMe)) {
		myTable->setLastPage (myTable->lastPage () + 1);
		lastPage = make_shared <MyDB_PageReaderWriter> (make_shared <MyDB_PageHandleBase> (myTable->lastPage()), myBufferManager->getPageSize());
		lastPage->clear ();
		lastPage->append (appendMe);
	}
}

// TBD(Modify more into my style)
void MyDB_TableReaderWriter :: loadFromTextFile (string fromMe) {
	myTable->setLastPage(0);

	// try to open the file
	string line;
	ifstream myfile;
	myfile.open(fromMe);

	// if we opened it, read the contents
	MyDB_RecordPtr tempRec = getEmptyRecord ();
	if (myfile.is_open()) {
		// loop through all of the lines
		while (getline (myfile,line)) {
			tempRec->fromString (line);		
			append (tempRec);
		}
		myfile.close ();
	}
}

// TBD
MyDB_RecordIteratorPtr MyDB_TableReaderWriter :: getIterator (MyDB_RecordPtr iterateIntoMe) {
	return nullptr;
}

// TBD
void MyDB_TableReaderWriter :: writeIntoTextFile (string toMe) {
	ofstream myfile;
	myfile.open(toMe);

	if(myfile.is_open()) {
		// get an empty record
		MyDB_RecordPtr tempRec = getEmptyRecord ();;		

		// and write out all of the records
		MyDB_RecordIteratorPtr myIter = getIterator (tempRec);
		while (myIter->hasNext ()) {
			myIter->getNext ();
		}
		output.close ();
	}
}

#endif

