
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <string>

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

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr, long) {
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
	/****************************
	
	unpinMe.unpin();

	****************************/
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile) {
	// this.buffer = malloc(pageSize * numPages);
	// this.LRU = new LRU();
	// this.lookUp = new HashMap<>();
}

MyDB_BufferManager :: ~MyDB_BufferManager () {
}
	
#endif


