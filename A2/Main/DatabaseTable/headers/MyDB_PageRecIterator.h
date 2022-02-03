//
// Created by Jingcheng Su on 2022/2/2.
//

#ifndef COMP530_CODE_MYDB_PAGERECITERATOR_H
#define COMP530_CODE_MYDB_PAGERECITERATOR_H

#include "MyDB_PageHandle.h"
#include "MyDB_Record.h"
#include "MyDB_RecordIterator.h"

class MyDB_PageRecIterator: public MyDB_RecordIterator {

public:
    bool hasNext() override;
    void getNext() override;

    MyDB_PageRecIterator(MyDB_PageHandle page, MyDB_RecordPtr record);
    ~MyDB_PageRecIterator() override;

private:
    MyDB_PageHandle page;
    MyDB_RecordPtr record;
    size_t offset;
};


#endif //COMP530_CODE_MYDB_PAGERECITERATOR_H
