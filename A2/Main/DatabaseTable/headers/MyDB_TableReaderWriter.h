
#ifndef TABLE_RW_H
#define TABLE_RW_H

#include <memory>
#include "MyDB_BufferManager.h"
#include "MyDB_Record.h"
#include "MyDB_RecordIterator.h"
#include "MyDB_Table.h"

using namespace std;

// create a smart pointer for the catalog
class MyDB_PageReaderWriter;
class MyDB_TableReaderWriter;
typedef shared_ptr <MyDB_TableReaderWriter> MyDB_TableReaderWriterPtr;
typedef shared_ptr<MyDB_PageReaderWriter> MyDB_PageReaderWriterPtr;

class MyDB_TableReaderWriter {

public:

	// ANYTHING ELSE YOU NEED HERE

	// create a table reader/writer for the specified table, using the specified
	// buffer manager
	MyDB_TableReaderWriter (MyDB_TablePtr forMe, MyDB_BufferManagerPtr myBuffer);

	// gets an empty record from this table
	MyDB_RecordPtr getEmptyRecord ();

	// append a record to the table
	void append (MyDB_RecordPtr appendMe);

	// return an itrator over this table... each time returnVal->next () is
	// called, the resulting record will be placed into the record pointed to
	// by iterateIntoMe
	MyDB_RecordIteratorPtr getIterator (MyDB_RecordPtr iterateIntoMe);

	// load a text file into this table... overwrites the current contents
	void loadFromTextFile (const string& fromMe);

	// dump the contents of this table into a text file
	void writeIntoTextFile (string toMe);

	// access the i^th page in this file
	MyDB_PageReaderWriter operator [] (size_t i);

	// access the last page in the file
	MyDB_PageReaderWriter last ();

	size_t lastPage();

private:

	// ANYTHING YOU NEED HERE
	friend class MyDB_PageReaderWriter;
	friend class MyDB_TableRecIterator;

	MyDB_TablePtr myTable;
	MyDB_BufferManagerPtr myBufferManager;
	
};

#endif
