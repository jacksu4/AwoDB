#ifndef LRU_C
#define LRU_C

#include <MyDB_LRU.h>
#include "MyDB_BufferManager.h"

using namespace std;

//Node::Node(MyDB_PagePtr page): page(page), next(nullptr), prev(nullptr) {
//}

//LRU::LRU(size_t capacity, MyDB_BufferManager &bufferManager): capacity(capacity), bufferManager(bufferManager), size(0) {
//    head = new Node(nullptr);
//    tail = new Node(nullptr);
//    head->next = tail;
//    tail->prev = head;
//}

LRU::~LRU() {

}

bool LRU::isFull() const {
    return this->size >= this->capacity;
}

void LRU::remove(Node *node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
}

void LRU::addToHead(Node *node) {
    node->next = node->prev->next;
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

//Node *LRU::findNode(const pair<MyDB_TablePtr, size_t>& id) {
//    if(map.find(id) != map.end()) {
//        Node *curr = map.find(id)->second;
//        moveToHead(curr);
//        return curr;
//    } else {
//        return nullptr;
//    }
//}

//Node *LRU::addToMap(pair<MyDB_TablePtr, size_t> id, MyDB_PagePtr page) {
//    //TODO: modify based on buffer manager
//    size++;
//    Node *node = new Node(page);
//    map[make_pair(id.first, id.second)] = node;
//    addToHead(node);
//    return node;
//}

int LRU::getSize() const {
    return this->size;
}

//void LRU::eraseNode(Node *node) {
//    remove(node);
//    //TODO: modify based on buffer manager
//}

Node::Node(int val): val(val), next(nullptr), prev(nullptr) {

}

LRU::LRU(size_t capacity, MyDB_BufferManager &bufferManager): capacity(capacity), bufferManager(bufferManager), size(0) {
    head = new Node(0);
    tail = new Node(0);
    head->next = tail;
    tail->prev = head;
}

Node *LRU::addToMap(int id, int val) {
    size++;
    Node *node = new Node(val);
    map[id] = node;
    addToHead(node);
    return node;
}

void LRU::eraseNode(Node *node) {
    remove(node);
    map.erase(node->val);
    //TODO: modify based on buffer manager
}

Node *LRU::findNode(int id) {
    if(map.find(id) != map.end()) {
        Node *curr = map.find(id)->second;
        moveToHead(curr);
        return curr;
    } else {
        return nullptr;
    }
}

#endif //LRU_C
