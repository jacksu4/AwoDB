//
// Created by Jingcheng Su on 2022/2/2.
//

#include "MyDB_TableRecIterator.h"


void MyDB_TableRecIterator::getNext() {
    curPageIter->getNext();
}

// TBD(Modify more into my style)
bool MyDB_TableRecIterator::hasNext() {
    if (curPageIter->hasNext ()) {
        return true;
    }
    if(curPage == (this->parent.myTable)->lastPage()) {
        return false;
    }

    curPage++;
	curPageIter = parent[curPage].getIterator (rec);
	return hasNext();
}

MyDB_TableRecIterator::MyDB_TableRecIterator(MyDB_TableReaderWriter &parent, MyDB_RecordPtr record) {
    this->parent = parent;
    this->rec = record;
    this->curPage = 0;
    this->curPageIter = parent[curPage].getIterator(record);

}

MyDB_TableRecIterator::~MyDB_TableRecIterator() = default;


