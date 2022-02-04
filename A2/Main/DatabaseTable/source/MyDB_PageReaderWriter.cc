
#ifndef PAGE_RW_C
#define PAGE_RW_C

#include "MyDB_PageReaderWriter.h"

#include <utility>
#include "MyDB_PageRecIterator.h"
#include "MyDB_PageType.h"

void MyDB_PageReaderWriter :: clear () {
    //access the size info part inside the page memory and set it to 4 (which contains only page type and current size info and has zero records)
    //and access the type inside page memory and set the type
    *((size_t *)((char *)page->getBytes() + sizeof(MyDB_PageType))) = sizeof(MyDB_PageType) + sizeof(size_t);
    *(MyDB_PageType *)(page->getBytes()) = RegularPage;
    page->wroteBytes();
}

MyDB_PageType MyDB_PageReaderWriter :: getType () {
	return (*(MyDB_PageType *)(page->getBytes()));
}

MyDB_RecordIteratorPtr MyDB_PageReaderWriter :: getIterator (MyDB_RecordPtr iterateIntoMe) {
	return make_shared<MyDB_PageRecIterator>(this->page, iterateIntoMe);
}

void MyDB_PageReaderWriter :: setType (MyDB_PageType toMe) {
    *(MyDB_PageType *)(page->getBytes()) = toMe; //access the type inside page memory and set the type
    page->wroteBytes();
}

bool MyDB_PageReaderWriter :: append (MyDB_RecordPtr appendMe) {
    size_t recSize = appendMe->getBinarySize();
    if (recSize > (pageSize - *(size_t *)((char *)page->getBytes() + sizeof(MyDB_PageType)))) { //pageSize minus page current size
        return false;
    }
    appendMe->toBinary(*((size_t *)((char *)page->getBytes() + sizeof(MyDB_PageType))) + (char *)page->getBytes());
    *((size_t *)((char *)page->getBytes() + sizeof(MyDB_PageType))) += recSize;
    page->wroteBytes();
	return true;
}

MyDB_PageReaderWriter::MyDB_PageReaderWriter(MyDB_PageHandle page, size_t pageSize) {
    this->page = std::move(page);
    this->pageSize = pageSize;
}

MyDB_PageReaderWriter::~MyDB_PageReaderWriter() {

}

#endif
