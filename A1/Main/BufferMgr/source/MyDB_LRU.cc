#ifndef LRU_C
#define LRU_C

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

void* LRU::usePage(MyDB_PagePtr p) {
    /*
    
    Node *node = findNode(p.getTable(), getOffset());

    if(*node != null) {
        this.moveToHead(node);
    } else {
        *node = new Node(p);
        if(this.isFull()) {
            if(this.size() == 0) {
                return null;
            }
            Node *evictNode = this.popTail();
            void *bytes = evictNode.page.getBytes();
            node.setBytes(bytes);
        } else {
            node.setBytes();// Randomly allocate a place in buffer for the page
        }
        this.addToHead(node);
    }

    return node.getBytes();

    */
    return 0;
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
    return lastNode;
    //TODO: add pin functionality
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
    //TODO: modify based on buffer manager
    size++;
    Node *node = new Node(page);
    map[make_pair(id.first, id.second)] = node;
    addToHead(node);
    return node;
}

int LRU::getSize() const {
    return this->size;
}

void LRU::eraseNode(Node *node) {
    remove(node);
    //TODO: modify based on buffer manager and delete the map entry
}

#endif //LRU_C
