//
// Created by Jingcheng Su on 2022/2/2.
//

#ifndef COMP530_CODE_MYDB_TABLERECITERATOR_H
#define COMP530_CODE_MYDB_TABLERECITERATOR_H

#include "MyDB_RecordIterator.h"
#include "MyDB_TableReaderWriter.h"

class MyDB_TableRecIterator: public MyDB_RecordIterator {

public:
    void getNext() override;
    bool hasNext() override;

    MyDB_TableRecIterator(MyDB_TableReaderWriter &parent, MyDB_TablePtr table, MyDB_RecordPtr record);
    ~MyDB_TableRecIterator() override;
};


#endif //COMP530_CODE_MYDB_TABLERECITERATOR_H
