
#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

#include "../headers/MyDB_PageHandle.h"
#include "../../Catalog/headers/MyDB_Table.h"
#include "MyDB_LRU.h"
#include <vector>

using namespace std;

class MyDB_BufferManager {

public:

	// THESE METHODS MUST APPEAR AND THE PROTOTYPES CANNOT CHANGE!

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle 
	// to that already-buffered page should be returned
	MyDB_PageHandle getPage (const MyDB_TablePtr& whichTable, long i);

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular 
	// table
	MyDB_PageHandle getPage ();

	// gets the i^th page in the table whichTable... the only difference 
	// between this method and getPage (whicTable, i) is that the page will be 
	// pinned in RAM; it cannot be written out to the file
	MyDB_PageHandle getPinnedPage (const MyDB_TablePtr& whichTable, long i);

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage ();

	// un-pins the specified page
	void unpin (MyDB_PageHandle unpinMe);

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize 
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager (size_t pageSize, size_t numPages, const string& tempFile);
	
	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~MyDB_BufferManager ();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS

	//get the buffer
	vector<void*> getBuffer();

	void killPage(MyDB_Page& page);

	void access(MyDB_Page& page);

private:

    friend class LRU;
    //tempFile for anonymous page
    string tempFile;

    //offset for anonymous page
    size_t anonymousCounter;

	// Lookup Table (contains both anon & unanon pages)
	map<pair<MyDB_TablePtr, size_t>, MyDB_PagePtr> lookupTable;

	// LRU
	LRU* lru;

	// Buffer (Available Ram)
	vector<void*> buffer;

	//Page Size
	size_t pageSize;

	//Number of pages
	size_t numPages;

	// Buffer Set (Indexes in the set indicates the available buffer)
	// set<int> bufferSet;

};

#endif


