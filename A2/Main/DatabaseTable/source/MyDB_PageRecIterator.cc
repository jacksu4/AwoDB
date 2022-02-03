//
// Created by Jingcheng Su on 2022/2/2.
//

#ifndef COMP530_CODE_MYDB_PAGERECITERATOR_C
#define COMP530_CODE_MYDB_PAGERECITERATOR_C

#include "MyDB_PageRecIterator.h"
#include "MyDB_PageType.h"
#include <string>
#include <iostream>

bool MyDB_PageRecIterator :: hasNext() {

};

void MyDB_PageRecIterator :: getNext() {

};

MyDB_PageRecIterator ::MyDB_PageRecIterator(MyDB_PageHandle page, MyDB_RecordPtr record) {
    this->page = page;
    this->record = record;
    this->offset = sizeof(size_t) * 2; //the header size?
}

MyDB_PageRecIterator ::~MyDB_PageRecIterator() = default;

#endif
