#ifndef PAGE_C
#define PAGE_C

#include "MyDB_Page.h"
#include "MyDB_BufferManager.h"

MyDB_Page::MyDB_Page(MyDB_TablePtr table, size_t offset, MyDB_BufferManager &bufferManager):
    table(table), offset(offset), bufferManager(bufferManager), bytes(nullptr), refCount(0),
    dirty(false), pinned(false){

}

MyDB_Page :: ~MyDB_Page() {
    //no need since reduceRefCount will deal with destruction
}

size_t MyDB_Page::getRefCount() const {
    return this->refCount;
}

void MyDB_Page::addRefCount() {
    this->refCount++;
}

void MyDB_Page::reduceRefCount() {
    this->refCount--;
    if (this->refCount == 0) {
        //TODO: bufferManager kill the page
    }
}

void *MyDB_Page::getBytes() {
    //TODO: bufferManager called to deal with getBytes
    return this->bytes;
}

void MyDB_Page::wroteBytes() {
    this->dirty = true;
}

bool MyDB_Page::isDirty() const {
    return this->dirty;
}

bool MyDB_Page::isPinned() const {
    return this->pinned;
}

void MyDB_Page::setDirty(bool d) {
    this->dirty = d;
}

void MyDB_Page::setPin(bool p) {
    this->pinned = p;
}

void MyDB_Page::setBytes(void *ram) {
    this->bytes = ram;
}

size_t MyDB_Page::getOffset() const {
    return this->offset;
}


#endif //PAGE_H

