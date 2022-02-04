//
// Created by Jingcheng Su on 2022/2/2.
//

#ifndef COMP530_CODE_MYDB_PAGERECITERATOR_C
#define COMP530_CODE_MYDB_PAGERECITERATOR_C

#include "MyDB_PageRecIterator.h"
#include "MyDB_PageType.h"
#include <string>
#include <iostream>
#include <utility>

bool MyDB_PageRecIterator :: hasNext() {
    if (this->offset < *((size_t *)((char *)page->getBytes() + sizeof(MyDB_PageType)))) { //check if offset is larger than the current page size
        return true;
    }
    return false;
};

void MyDB_PageRecIterator :: getNext() {
    if(hasNext()) {
        char* curAddr = offset + ((char *)(page->getBytes())); //current position inside the page
        char *nexAddr = (char *) record->fromBinary(curAddr); //see fromBinary comment for explanation. Basically get the next record address
        offset += nexAddr - curAddr;
    }
};

MyDB_PageRecIterator ::MyDB_PageRecIterator(MyDB_PageHandle page, MyDB_RecordPtr record) {
    this->page = std::move(page);
    this->record = std::move(record);
    this->offset = sizeof(MyDB_PageType) + sizeof(size_t); //the page header size: contains pageType info and current size info
}

MyDB_PageRecIterator ::~MyDB_PageRecIterator() = default;

#endif
