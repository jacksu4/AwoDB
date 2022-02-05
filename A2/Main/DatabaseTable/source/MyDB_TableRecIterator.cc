//
// Created by Jingcheng Su on 2022/2/2.
//

#include "MyDB_TableRecIterator.h"
#include "MyDB_PageReaderWriter.h"
#include <utility>


void MyDB_TableRecIterator::getNext() {
    curPageIter->getNext();
}

bool MyDB_TableRecIterator::hasNext() {
    if (curPageIter->hasNext ()) {
        return true;
    }
    while(curPage < table->lastPage()) {
        curPageIter = parent[++curPage].getIterator(rec);
        if(curPageIter->hasNext()) {
            return true;
        }
    }
    return false;
}

MyDB_TableRecIterator::MyDB_TableRecIterator(MyDB_TableReaderWriter &parent, MyDB_TablePtr table,
                                             MyDB_RecordPtr record): parent(
        (MyDB_TableReaderWriter &) std::move(parent)),
                                             table(std::move(table)), rec(std::move(record)) {
    this->curPage = 0;
    this->curPageIter = this->parent[curPage].getIterator(rec);
}


MyDB_TableRecIterator::~MyDB_TableRecIterator() = default;


