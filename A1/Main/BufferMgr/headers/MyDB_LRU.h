#ifndef LRU_H
#define LRU_H

#include <map>
#include <list>
#include "MyDB_Page.h"

using namespace std;

class Node {

public:
    explicit Node(MyDB_PagePtr page);

    Node *next;

    Node *prev;

    MyDB_PagePtr page;

};

class LRU {

public:
    //LRU constructor
    LRU(size_t capacity, MyDB_BufferManager& bufferManager);

    ~LRU();

    /*
    
    // Return the index of the buffer to store data
    
    void remove(Page* p);

    */

    bool isFull() const;

    void remove(Node *node);

    void addToHead(Node *node);

    void moveToHead(Node *Node);

    Node* popTail();

    Node* findNode(const pair<MyDB_TablePtr, size_t>& id);

    Node* addToMap(const pair<MyDB_TablePtr, size_t>& id, MyDB_PagePtr page);

    int getSize() const;

    void eraseNode(Node *node);

private:

    friend class MyDB_BufferManager;

    //capacity of LRU
    size_t capacity;

    //buffer manager
    MyDB_BufferManager& bufferManager;

    //current size of LRU
    size_t size;

    //map of the LRU
    map<pair<MyDB_TablePtr, size_t>, Node*> map;

    Node *head;

    Node *tail;
};

#endif //LRU_H
