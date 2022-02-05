
#ifndef TABLE_RW_C
#define TABLE_RW_C

#include <fstream>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_RecordIterator.h"

using namespace std;

MyDB_TableReaderWriter :: MyDB_TableReaderWriter (MyDB_TablePtr forMe, MyDB_BufferManagerPtr myBuffer) {
	myTable = forMe;
	myBufferManager = myBuffer;	
	if(myTable->lastPage() == -1) {
		myTable->setLastPage(0);
        (*this)[0].clear();
	}
}

MyDB_PageReaderWriter MyDB_TableReaderWriter :: operator [] (size_t i) {
	MyDB_PageHandle page = myBufferManager->getPage(myTable, i);
    while(i > myTable->lastPage()) {
        myTable->setLastPage(myTable->lastPage() + 1);
        auto tmp = make_shared<MyDB_PageReaderWriter>(page, myBufferManager->getPageSize());
        tmp->clear();
    }
    return *make_shared<MyDB_PageReaderWriter>(page, myBufferManager->getPageSize());
}

MyDB_RecordPtr MyDB_TableReaderWriter :: getEmptyRecord () {
	return make_shared <MyDB_Record> (myTable->getSchema());
}

MyDB_PageReaderWriter MyDB_TableReaderWriter :: last () {
	return (*this)[myTable->lastPage()];
}


void MyDB_TableReaderWriter :: append (MyDB_RecordPtr appendMe) {
	// try to append the record on the current page...
	while(!this->last().append(appendMe)) {
        myTable->setLastPage(myTable->lastPage() + 1);
        (*this)[myTable->lastPage() + 1].clear();
    }
}

void MyDB_TableReaderWriter :: loadFromTextFile (const string& fromMe) {
	myTable->setLastPage(0);

	// try to open the file
	string line;
	ifstream file(fromMe);

	// if we opened it, read the contents
	if (file.is_open()) {
        auto rec = getEmptyRecord();
		// loop through all of the lines
		while (getline(file,line)) {
			rec->fromString (line);
			append(rec);
		}
        file.close();
	}
}

MyDB_RecordIteratorPtr MyDB_TableReaderWriter :: getIterator (MyDB_RecordPtr iterateIntoMe) {
    return make_shared<MyDB_TableRecIterator>(*this, myTable, iterateIntoMe);
}

// TBD
void MyDB_TableReaderWriter :: writeIntoTextFile (string toMe) {
	ofstream file(toMe);

    if(file.is_open()) {
        auto recIter = getIterator(getEmptyRecord());
        while(recIter->hasNext()) {
            recIter->getNext();
        }
        file.close();
    }
}

#endif

