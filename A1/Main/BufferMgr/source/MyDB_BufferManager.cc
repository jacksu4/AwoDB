
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <string>
#include <vector>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>

using namespace std;

MyDB_PageHandle MyDB_BufferManager :: getPage (const MyDB_TablePtr& whichTable, long i) {

    pair<MyDB_TablePtr, size_t> idx = make_pair(whichTable, i);

    if (!lookupTable.count(idx)) {
        MyDB_PagePtr new_page = make_shared<MyDB_Page>(whichTable, i, *this);
        lookupTable[idx] = new_page;
    }

    MyDB_PagePtr page = lookupTable[idx];
    return make_shared<MyDB_PageHandleBase>(page);
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {

    //TODO: may need to extend the size of temp file

    MyDB_PagePtr new_page = make_shared<MyDB_Page>(nullptr, anonymousCounter, *this);
    lookupTable[make_pair(nullptr, anonymousCounter)] = new_page;
    anonymousCounter++;

    return make_shared<MyDB_PageHandleBase>(new_page);
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (const MyDB_TablePtr& whichTable, long i) {
    pair <MyDB_TablePtr, long> idx = make_pair (whichTable, i);
    
    if (!lookupTable.count(idx)) {
        MyDB_PagePtr new_page = make_shared<MyDB_Page>(whichTable, i, *this);
        lookupTable[idx] = new_page;
    }
    MyDB_PagePtr page = lookupTable[make_pair(whichTable, i)];
    page->setPin(true);

    if(page->bytes == nullptr) {
        if(lru->isFull()) {
            lru->popTail();
        }
        lru->addToMap(idx, page);
        page->bytes = buffer[buffer.size() - 1];
        buffer.pop_back();
    }

    return make_shared<MyDB_PageHandleBase>(page);
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {

    if(lru->isFull()) {
        lru->popTail();
    }

    MyDB_PagePtr new_page = make_shared<MyDB_Page>(nullptr, anonymousCounter, *this);
    lookupTable[make_pair(nullptr, anonymousCounter)] = new_page;
    lru->addToMap(make_pair(nullptr, anonymousCounter), new_page);
    new_page->setPin(true);

    anonymousCounter++;
    new_page->bytes = buffer[buffer.size() - 1];
    buffer.pop_back();

    return make_shared<MyDB_PageHandleBase>(new_page);
}

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinMe) {
	unpinMe->getPage()->setPin(false);

	MyDB_PagePtr page = unpinMe->getPage();

	Node* node = lru->map[make_pair(page->getTable(), page->getOffset())];

	lru->addToHead(node);

    lru->capacity++;
}

void MyDB_BufferManager :: killPage(MyDB_Page& page) {

    pair<MyDB_TablePtr, size_t> lookUpKey = make_pair(page.getTable(), page.getOffset());

    if(page.getTable() == nullptr) {
        anonymousCounter--;
    }

    //if the page is pinned, just unpin it
    if(lookupTable.count(lookUpKey) && page.isPinned()) {
        MyDB_PagePtr ptr = lookupTable[lookUpKey];
        MyDB_PageHandle tmp = make_shared<MyDB_PageHandleBase>(ptr);
        unpin(tmp);
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
        write(fd, page.bytes, pageSize);
        page.setDirty(false);
        close(fd);
    }

    //remove from LRU (may or may not needed) need to put after file operation because it will set
    //the page->bytes to nullptr
    if(lru->findNode(lookUpKey) != nullptr) {
        lru->eraseNode(lru->findNode(lookUpKey));
    }

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
        if (page->bytes != nullptr) {
            int fd;
            if(page->isDirty()) {
                if (page->getTable() == nullptr) {
                    fd = open(tempFile.c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);
                } else {
                    fd = open(page->getTable()->getStorageLoc().c_str(), O_CREAT | O_RDWR | O_FSYNC, 0666);
                }
                lseek(fd, page->getOffset() * this->pageSize, SEEK_SET);
                write(fd, page->bytes, pageSize);
                page->setDirty(false);
                close(fd);
            }
//            free(page->bytes);
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
    if (lru->findNode(key)) {
        if(!page.isPinned()) {
            lru->moveToHead(lru->findNode(key));
        }
    } else {
        if(lru->isFull() && lru->getSize() == 0) {
            return;
        }
        if(lru->isFull()) {
            lru->popTail();
        }
        Node* node = lru->addToMap(key, lookupTable[key]);
    }

    if (page.bytes == nullptr) {
        page.setBytes(buffer[buffer.size() - 1]);
        buffer.pop_back();
    }

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


