#ifndef LRU_C
#define LRU_C

#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>
#include <MyDB_LRU.h>
#include "MyDB_BufferManager.h"

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
}

void LRU::moveToHead(Node *node) {
    this->remove(node);
    this->addToHead(node);
}

Node *LRU::popTail() {
    Node *lastNode = tail->prev;
    this->remove(lastNode);

    MyDB_BufferManager manager = this->bufferManager;
    // delete the corresponding entry in map
    MyDB_PagePtr curPage = lastNode->page;
    MyDB_TablePtr table = curPage->getTable();
    pair<MyDB_TablePtr, size_t> pageNode = make_pair(table, (curPage->getOffset()));
    this->map.erase(pageNode);

    if(lastNode->page->isDirty()) {
        if(!table) {
            if(manager.openFile.count(nullptr) == 0) {
                int file = open(manager.tempFile.c_str (), O_TRUNC | O_CREAT | O_RDWR | O_FSYNC, 0666);
                manager.openFile[nullptr] = file;
            }
        }

        if(manager.openFile.count(table) == 0) {
            int file = open(table->getStorageLoc().c_str (), O_TRUNC | O_CREAT | O_RDWR | O_FSYNC, 0666);
            manager.openFile[table] = file;
        }

        lseek(manager.openFile[table], curPage->getOffset() * manager.pageSize, SEEK_SET);
        write(manager.openFile[table], curPage->getBytes(), manager.pageSize);
    }
    /*

    map.erase(pair<lastNode->getPage()->getTable(), lastNode->getPage()->getOffset()>);
    if(lastNode.getPage()->isDirty()) {
        MyDB_TablePtr table = lastNode.getPage()->getTable();
        if(table == nullptr) {
            write(bufferManager.tempFile);
            return lastNode;
        }
        write(lastNode.getPage()->getTable());
    }
    return lastNode;
    */
    return lastNode;
}

Node *LRU::findNode(const pair<MyDB_TablePtr, size_t>& id) {
    if(map.find(id) != map.end()) {
        Node *curr = map.find(id)->second;
        moveToHead(curr);
        return curr;
    } else {
        return nullptr;
    }
}

Node *LRU::addToMap(pair<MyDB_TablePtr, size_t> id, MyDB_PagePtr page) {
    size++;
    Node *node = new Node(page);
    map[make_pair(id.first, id.second)] = node;
    return node;
}

int LRU::getSize() const {
    return this->size;
}

void LRU::eraseNode(Node *node) {
    remove(node);
    MyDB_PagePtr curPage = node->page;
    pair<MyDB_TablePtr, size_t> curPair = make_pair(curPage->getTable(), curPage->getOffset());
    this->map.erase(curPair);
    /*
    this->map.erase(pair<node->getPage()->getTable(), node->getPage()->getOffset()>);// done
    this->bufferManager.buffer.pushBack(node.getPage()->getBytes());
    */
    this->bufferManager.buffer.push_back(curPage->getBytes());
}

#endif //LRU_C