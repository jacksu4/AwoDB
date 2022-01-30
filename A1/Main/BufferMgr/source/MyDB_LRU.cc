#ifndef LRU_C
#define LRU_C

#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>
#include <MyDB_LRU.h>
#include "MyDB_BufferManager.h"


#include <iostream>

using namespace std;

Node::Node(MyDB_PagePtr page): page(page), next(nullptr), prev(nullptr) {
}

LRU::LRU(size_t capacity, MyDB_BufferManager &bufferManager): capacity(capacity), bufferManager(bufferManager), size(0) {
    head = new Node(nullptr);
    tail = new Node(nullptr);
    head->next = tail;
    tail->prev = head;
}

LRU::~LRU() {
    delete head;
    delete tail;
}

bool LRU::isFull() const {
    return this->size >= this->capacity;
}

void LRU::remove(Node *node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
    node->prev = nullptr;
    node->next = nullptr;
    this->size--;
}

void LRU::addToHead(Node *node) {
    if(this->isFull()) {
        return;
    }
    node->prev = head;
    node->next = head->next;
    head->next->prev = node;
    head->next = node;

    this->size++;
}

void LRU::moveToHead(Node *node) {
    this->remove(node);
    this->addToHead(node);
}

Node *LRU::popTail() {
    Node *lastNode = tail->prev;

    for(int i = 0; i < this->capacity; i++) {
        if(!lastNode->page->isPinned()) {
            break;
        }
        lastNode = lastNode->prev;
    }

    if(lastNode == head) {
        //undefined behavior
        std::cout<<"lru is full of pinned page!"<<std::endl;
        exit(1);
    }

    this->remove(lastNode);

    // delete the corresponding entry in map
    MyDB_PagePtr curPage = lastNode->page;
    MyDB_TablePtr table = curPage->getTable();

    if(lastNode->page->isDirty()) {
        int file;
        if(!table) {
            file = open(this->bufferManager.tempFile.c_str (), O_CREAT | O_RDWR, 0666);
        } else {
            file = open(table->getStorageLoc().c_str (), O_CREAT | O_RDWR, 0666);
        }

        lseek(file, curPage->getOffset() * this->bufferManager.pageSize, SEEK_SET);
        write(file, curPage->bytes, this->bufferManager.pageSize);
        close(file);
        lastNode->page->setDirty(false);
    }

    eraseNode(lastNode);

    return lastNode;
}

Node *LRU::findNode(const pair<MyDB_TablePtr, size_t>& id) {
    if(map.find(id) != map.end()) {
        Node *curr = map.find(id)->second;
        return curr;
    } else {
        return nullptr;
    }
}

Node *LRU::addToMap(const pair<MyDB_TablePtr, size_t>& id, MyDB_PagePtr page) {
    Node *node = new Node(page);
    map[make_pair(id.first, id.second)] = node;

    addToHead(node);
//    if(!(node->page->isPinned())) {
//        addToHead(node);
//    } else {
//        this->capacity--;
//    }
    return node;
}

int LRU::getSize() const {
    return this->size;
}

void LRU::eraseNode(Node *node) {
    if(node->prev && node->next) {
        remove(node);
    }

    MyDB_PagePtr curPage = node->page;
    pair<MyDB_TablePtr, size_t> curPair = make_pair(curPage->getTable(), curPage->getOffset());
    this->map.erase(curPair);
    this->bufferManager.buffer.push_back(curPage->bytes);
    curPage->bytes = nullptr;
}

#endif //LRU_C