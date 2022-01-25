
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <string>
#include <vector>

using namespace std;

MyDB_PageHandle MyDB_BufferManager :: getPage (MyDB_TablePtr whichTable, long i) {
	/****************************
	
	if(lru.isFull() && lru.size() == 0) {
		return null;
	}

	if(!lookUp.contains(<whichTable, i>)) {
		p = new Page(whichTable, i, this);
		lookUp.put(<whichTable, i>, p);
	}

	p = lookUp.get(<whichTable, i>);

	Node *node = findNode(p.getTable(), p.getOffset());

    if(*node != null) {
        if(!p.isPinned()) {
            lru.moveToHead(node);
        }
    } else {
        if(lru.isFull()) {
            Node *evictNode = lru.popTail();
            void *bytes = evictNode.page.getBytes();
			evictNode.page.setBytes(nullptr);
            p.setBytes(bytes);
        } else {
            p.setBytes(buffer[buffer.size() - 1]);// Randomly allocate a place in buffer for the page
			buffer.popBack();
        }
		Node *node = lru.addToMap(pair<p.getTable(), p.getOffset()>, p);
		lru.addToHead(node);

		void* bytes = p.getBytes();
		readFromFile(bytes);
    }

	// Manage p.refCount++ in constructor
	ph = new PageHandleBase(p);

	return ph;

	****************************/
	return nullptr;		
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {
	/****************************
	
	if(lru.isFull() && lru.size() == 0) {
		return null;
	}

	p = new Page(tempFile, anonymousCounter, this);
	anonymousCounter++;

	if(lru.isFull()) {
		Node *evictNode = lru.popTail();
		void *bytes = evictNode.page.getBytes();
		evictNode.page.setBytes(nullptr);
		p.setBytes(bytes);
	} else {
		p.setBytes(buffer[buffer.size() - 1]);// Randomly allocate a place in buffer for the page
		buffer.popBack();
	}
	Node *node = lru.addToMap(<p.getTable(), p.getOffset()>, p);
	lru.addToHead(node);

	ph = new PageHandleBase(p);

	return ph;

	****************************/
	return nullptr;		
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (const MyDB_TablePtr&, long) {
	/****************************
	
	if(lru.isFull() && lru.size() == 0) {
		return null;
	}

	if(!lookUp.contains(<whichTable, i>)) {
		p = new Page(whichTable, i, this);

		lookUp.put(<whichTable, i>, p);
	}

	p = lookUp.get(<whichTable, i>);

	Node *node = findNode(p.getTable(), getOffset());

	if(*node != null) {
		if(!p.isPinned()) {
			lru.remove(node);
		}
	} else {
		lru.addToMap(<p.getTable(), p.getOffset()>, p);
	}

	// Manage p.refCount++ in constructor
	ph = new PageHandleBase(p);

	return ph;

	****************************/
	return nullptr;		
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
	/****************************
	
	if(lru.isFull() && lru.size() == 0) {
		return null;
	}

	p = new Page(tempFile, anonymousCounter, this);
	anonymousCounter++;

	if(lru.isFull()) {
		Node *evictNode = lru.popTail();
		void *bytes = evictNode.page.getBytes();
		evictNode.page.setBytes(nullptr);
		p.setBytes(bytes);
	} else {
		p.setBytes(buffer[buffer.size() - 1]);// Randomly allocate a place in buffer for the page
		buffer.popBack();
	}
	lru.addToMap(<p.getTable(), p.getOffset()>, p);

	ph = new PageHandleBase(p);

	return ph;

	****************************/
	return nullptr;		
}

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinMe) {

	unpinMe->getPage()->setPin(false);

	/*
	
	page = unpinMe.getPage();
	Node *node = lru->map[<page.getTable(), page.getOffSet()>];
	lru.addToHead(node);

	*/
	// put it back into LRU first position
}

void killPage(MyDB_PagePtr page) {
	/*

	pair<MyDB_TablePtr, size_t> lookUpKey = make_pair(page.getTable(), page.getOffset());

	if(page.getTable() == nullptr) {
		if(lru.map.contains(lookUpKey)) {
			Node *node = lru.map.get(lookUpKey);
			lru.eraseNode(node);
		}
		// xigou
		return;
	}
	
	lookupTable.erase(lookUpKey);
	if(page.isDirty()) {
		write(page);
	}
	// xigou

	return;
	*/
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


