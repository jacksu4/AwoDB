//
// Created by Jingcheng Su on 2022/2/2.
//

#include "MyDB_TableRecIterator.h"


void MyDB_TableRecIterator::getNext() {

}

bool MyDB_TableRecIterator::hasNext() {
    return false;
}

MyDB_TableRecIterator::MyDB_TableRecIterator(MyDB_TableReaderWriter &parent, MyDB_TablePtr table,
                                             MyDB_RecordPtr record) {

}

MyDB_TableRecIterator::~MyDB_TableRecIterator() = default;


