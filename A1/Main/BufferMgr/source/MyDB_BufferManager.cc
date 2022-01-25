
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <string>
#include <vector>

using namespace std;

MyDB_PageHandle MyDB_BufferManager :: getPage (MyDB_TablePtr whichTable, long i) {
	/****************************
	
	ph = null;
	void* bytes = (read from the file);
	if(!lookUp.contains(<whichTable, i>)) {
		p = new Page(whichTable, i, this);

		// Initialize
		p.dirty = false;
		p.pinned = false;
		p.refCount = 0;

		lookUp.put(<whichTable, i>, p);
	}

	p = lookUp.get(<whichTable, i>);
	// Check if buffer is full, evict
	buffer.usePage(p);

	// Check if LRU is full, evict and set pn to the head
	// should be done in the function putHead(Node pn);
	void* pos = LRU.usePage(p);

	*pos = (read from the file);
	p.bytes = pos;
	
	// Manage p.refCount++ in constructor
	ph = new PageHandleBase(p);

	return ph;


	****************************/
	return nullptr;		
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {
	/****************************
	
	p = new Page(whichTable, i, this);

	// Initialize
	p.dirty = false;
	p.pinned = false;
	p.refCount = 0;

	void* pos = LRU.usePage(p);

	*pos = (read from the file);
	p.bytes = pos;

	ph = new PageHandleBase(p);

	return ph;

	****************************/
	return nullptr;		
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (const MyDB_TablePtr&, long) {
	/****************************
	
	ph = null;
	void* bytes = (read from the file);
	if(!lookUp.contains(<whichTable, i>)) {
		p = new Page(whichTable, i, this);

		// Initialize
		p.dirty = false;
		p.pinned = true;
		p.refCount = 0;

		lookUp.put(<whichTable, i>, p);
	}

	p = lookUp.get(<whichTable, i>);
	p.pinned = true;
	// Check if buffer is full, evict
	buffer.usePage(p);

	// Check if LRU is full, evict and set pn to the head
	// should be done in the function putHead(Node pn);
	void* pos = LRU.usePage(p);

	*pos = (read from the file);
	p.bytes = pos;
	
	// Manage p.refCount++ in constructor
	ph = new PageHandleBase(p);

	return ph;


	****************************/
	return nullptr;		
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
	/****************************
	
	p = new Page(whichTable, i, this);

	// Initialize
	p.dirty = false;
	p.pinned = true;
	p.refCount = 0;

	void* pos = LRU.usePage(p);

	*pos = (read from the file);
	p.bytes = pos;

	ph = new PageHandleBase(p);

	return ph;

	****************************/
	return nullptr;		
}

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinMe) {
	unpinMe->getPage()->setPin(false);
	// put it back into LRU first position
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pageSize, size_t numPages, const string& tempFile) {
    this->pageSize = pageSize;
    this->numPages = numPages;
    this->tempFile = tempFile;
    this->anonymousCounter = 0;
    this->lru = new LRU(numPages, *this);
    this->lookupTable = {};

    for(size_t i = 0; i < numPages; i++) {
        this->buffer.push_back((void*)malloc(pageSize));
    }

}

MyDB_BufferManager :: ~MyDB_BufferManager () {
}

vector<void *> MyDB_BufferManager::getBuffer() {
    return this->buffer;
}

void MyDB_BufferManager::usePage(const MyDB_TablePtr& t, size_t offset) {
}

#endif


