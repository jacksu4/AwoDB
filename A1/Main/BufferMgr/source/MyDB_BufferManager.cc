
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <string>
#include <vector>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>

using namespace std;

MyDB_PageHandle MyDB_BufferManager :: getPage (const MyDB_TablePtr& whichTable, long i) {
    if (lru->isFull() && lru->size == 0) {
        //full of pinned page, then undefined behavior
        exit(1);
    }

    if (!lookupTable.count(make_pair(whichTable, i))) {
        MyDB_PagePtr new_page = make_shared<MyDB_Page>(whichTable, i, *this);
        lookupTable[make_pair(whichTable, i)] = new_page;
    }

    MyDB_PagePtr page = lookupTable[make_pair(whichTable, i)];

    Node* node = lru->findNode(make_pair(page->getTable(), page->getOffset()));

    if (node != nullptr) {
        if(!page->isPinned()) {
            lru->moveToHead(node);
        }
    } else {
        if (lru->isFull()) {
            Node* evictedNode = lru->popTail();
            void* bytes = evictedNode->page->getBytes();
            evictedNode->page->setBytes(nullptr);
            page->setBytes(bytes);
        } else {
            page->setBytes(buffer[buffer.size() - 1]);
            buffer.pop_back();
        }
        node = lru->addToMap(make_pair(page->getTable(), page->getOffset()), page);
        lru->addToHead(node);

        //how to deal with it
        void* bytes = page->getBytes();

        int fd;

        fd = open(page->getTable()->getStorageLoc().c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);
        lseek(fd, page->getOffset() * this->pageSize, SEEK_SET);
        read(fd, page->getBytes(), pageSize);
        close(fd);

    }
    return make_shared<MyDB_PageHandleBase>(page);
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
    if (lru->isFull() && lru->size == 0) {
        //full of pinned page, then undefined behavior
        exit(1);
    }

    MyDB_PagePtr new_page = make_shared<MyDB_Page>(nullptr, anonymousCounter, *this);
    lookupTable[make_pair(nullptr, anonymousCounter)] = new_page;
    anonymousCounter++;

    if (lru->isFull()) {
        Node* evictedNode = lru->popTail();
        void* bytes = evictedNode->page->getBytes();
        evictedNode->page->setBytes(nullptr);
        new_page->setBytes(bytes);
    } else {
        new_page->setBytes(buffer[buffer.size() - 1]);
        buffer.pop_back();
    }
    return make_shared<MyDB_PageHandleBase>(new_page);
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

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (const MyDB_TablePtr& whichTable, long i) {
    if (lru->isFull() && lru->size == 0) {
        //full of pinned page, then undefined behavior
        exit(1);
    }

    if (!lookupTable.count(make_pair(whichTable, i))) {
        MyDB_PagePtr new_page = make_shared<MyDB_Page>(whichTable, i, *this);
        lookupTable[make_pair(whichTable, i)] = new_page;
    }

    MyDB_PagePtr page = lookupTable[make_pair(whichTable, i)];

    Node* node = lru->findNode(make_pair(page->getTable(), page->getOffset()));

    if (node != nullptr && !page->isPinned()) {
        lru->remove(node);
    } else {
        lru->addToMap(make_pair(page->getTable(), page->getOffset()), page);
    }
    return make_shared<MyDB_PageHandleBase>(page);
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
    if (lru->isFull() && lru->size == 0) {
        //full of pinned page, then undefined behavior
        exit(1);
    }

    MyDB_PagePtr new_page = make_shared<MyDB_Page>(nullptr, anonymousCounter, *this);
    lookupTable[make_pair(nullptr, anonymousCounter)] = new_page;
    anonymousCounter++;

    if (lru->isFull()) {
        Node* evictedNode = lru->popTail();
        void* bytes = evictedNode->page->getBytes();
        evictedNode->page->setBytes(nullptr);
        new_page->setBytes(bytes);
    } else {
        new_page->setBytes(buffer[buffer.size() - 1]);
        buffer.pop_back();
    }
    lru->addToMap(make_pair(new_page->getTable(), new_page->getOffset()), new_page);
    return make_shared<MyDB_PageHandleBase>(new_page);
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

	MyDB_PagePtr page = unpinMe->getPage();

	Node* node = lru->map[make_pair(page->getTable(), page->getOffset())];

	lru->addToHead(node);

	/*
	
	page = unpinMe.getPage();
	Node *node = lru->map[<page.getTable(), page.getOffSet()>];
	lru.addToHead(node);

	*/
	// put it back into LRU first position
}

void MyDB_BufferManager :: killPage(MyDB_Page& page) {
    pair<MyDB_TablePtr, size_t> lookUpKey = make_pair(page.getTable(), page.getOffset());

    if(page.getTable() == nullptr) {
        if(lru->map.count(lookUpKey)) {
            Node* node = lru->map[lookUpKey];
            lru->eraseNode(node);
        }
        //xigou
        return;
    }
    lookupTable.erase(lookUpKey);

    int fd;
    if(page.isDirty()) {
        if (page.getTable() == nullptr) {
            fd = open(tempFile.c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);
        } else {
            fd = open(page.getTable()->getStorageLoc().c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);
        }
        lseek(fd, page.getOffset() * this->pageSize, SEEK_SET);
        write(fd, page.getBytes(), pageSize);
        page.setDirty(false);
        close(fd);
    }

    if (page.getBytes() != nullptr) {
        buffer.push_back(page.getBytes());
        page.setBytes(nullptr);
    }
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
        this->buffer.push_back(malloc(pageSize));
    }

}

MyDB_BufferManager :: ~MyDB_BufferManager () {
    for(const auto& entry: lookupTable) {
        MyDB_PagePtr page = entry.second;
        if (page->getBytes() != nullptr) {
            if (page->isDirty()) {
                int fd;
                if (page->getTable() == nullptr) {
                    fd = open(tempFile.c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);
                } else {
                    fd = open(page->getTable()->getStorageLoc().c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);
                }
                lseek(fd, page->getOffset() * this->pageSize, SEEK_SET);
                write(fd, page->getBytes(), pageSize);
                page->setDirty(false);
                close(fd);
            }
            free(page->getBytes());
            page->setBytes(nullptr);
        }
    }

    for (auto mem: buffer) {
        free(mem);
    }

    unlink(tempFile.c_str());
}

vector<void *> MyDB_BufferManager::getBuffer() {
    return this->buffer;
}

void MyDB_BufferManager::access(MyDB_Page &page) {
    pair<MyDB_TablePtr, size_t> key = make_pair(page.getTable(), page.getOffset());
    if (!lru->findNode(key)) {
        if (lru->isFull() && lru->size == 0) {
            exit(1);
        }
        if(!page.isPinned()) {
          Node* node = lru->addToMap(key, lookupTable[make_pair(page.getTable(), page.getOffset())]);
          lru->moveToHead(node);
        }
    }

    page.setBytes(buffer[buffer.size() - 1]);
    buffer.pop_back();

    int fd;
    if (page.getTable() == nullptr) {
        fd = open(tempFile.c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);
    } else {
        fd = open(page.getTable()->getStorageLoc().c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);
    }
    lseek(fd, page.getOffset() * pageSize, SEEK_SET);
    read(fd, page.bytes, pageSize);
    close(fd);
}

#endif


