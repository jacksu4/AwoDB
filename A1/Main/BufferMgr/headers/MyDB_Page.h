#ifndef PAGE_H
#define PAGE_H

#include "../../Catalog/headers/MyDB_Table.h"

class MyDB_Page;
typedef shared_ptr<MyDB_Page> MyDB_PagePtr;

class MyDB_BufferManager;

class MyDB_Page {

public:
    //constructor
    MyDB_Page(MyDB_TablePtr table, size_t offset, MyDB_BufferManager &bufferManager);
    //destructor
    ~MyDB_Page();

    size_t getRefCount() const;

    void addRefCount();

    void reduceRefCount();

    void* getBytes();

    void wroteBytes();

    bool isDirty() const;

    bool isPinned() const;

    void setDirty(bool d);

    void setPin(bool p);

    void setBytes(void* ram);

    size_t getOffset() const;

    MyDB_TablePtr getTable() const;

    //pointer to raw bytes
    void *bytes;


private:

    //parent buffer manager
    MyDB_BufferManager &bufferManager;

    //parent table
    MyDB_TablePtr table;

    //position of the page
    size_t offset;

    //whether it has been written
    bool dirty;

    //whether the page is pinned
    bool pinned;

    //number of reference to the page
    size_t refCount;
};

#endif //PAGE_H
