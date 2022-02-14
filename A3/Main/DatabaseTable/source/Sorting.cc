
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"

#include "RecordComparator.h"


using namespace std;

void mergeIntoFile(MyDB_TableReaderWriter &sortIntoMe, vector <MyDB_RecordIteratorAltPtr> &mergeUs, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	std::priority_queue<MyDB_RecordIteratorAltPtr, vector<MyDB_RecordIteratorAltPtr>, RecordComparator> pq(RecordComparator(comparator, lhs, rhs));
    for (auto single: mergeUs) {
        if (single->advance()) {
            pq.push(single);
        }
        while(!pq.empty()) {
            auto record = sortIntoMe.getEmptyRecord();
            auto cur = pq.top();
            cur->getCurrent(record);
            pq.pop();
            sortIntoMe.append(record);
            if(cur->advance()) {
                pq.push(cur);
            }
        }
    }
}

vector<MyDB_PageReaderWriter> mergeIntoList(MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter, MyDB_RecordIteratorAltPtr rightIter, function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	vector<MyDB_PageReaderWriter> ret;
	MyDB_PageReaderWriter curPage = MyDB_PageReaderWriter(*parent);
	/*
	Several cases in the loop:
	1. Only lhs has next
	2. Only rhs has next
	3. Both lhs and rhs has next, but lhs ordered first
	4. Both lhs and rhs has next, but rhs ordered first

	If last case belongs to cases 1 and 3, we need to load next record in lhs
	If last case belongs to cases 2 and 4, we need to load next record in rhs
	*/
	bool leftLast = true;
	bool rightLast = true;
	bool leftCheck = true;
	bool rightCheck = true;
	while(leftCheck || rightCheck) {
		if(leftCheck && leftLast) { // The first time to load or last case was 1 or 3
			leftIter->getCurrent(lhs);
			leftLast = false;
		}
		if(rightCheck && rightLast) { // The first time to load or last case was 2 or 4
			rightIter->getCurrent(rhs);
			rightLast = false;
		}
		
		bool compareRes = false;
		if(leftCheck && rightCheck) {// If both two records available
			compareRes = comparator();
		}

		if(
			(leftCheck && !rightCheck) // Case 1
			|| 
			(leftCheck && rightCheck && compareRes)// Case 3
			) {
			if(!curPage.append(lhs)) {
				ret.push_back(curPage);
				curPage = MyDB_PageReaderWriter (*parent);
				curPage.append(lhs);
			}
			leftCheck = leftIter->advance();
			leftLast = true;
			continue;
		}

		// If not cases 1 or 3, that is case 2 or 4
		rightIter->getCurrent(rhs);
		if(!curPage.append(rhs)) {
			ret.push_back(curPage);
			curPage = MyDB_PageReaderWriter (*parent);
			curPage.append(rhs);
		}
		rightCheck = rightIter->advance();
		rightLast = true;
	}
    ret.push_back(curPage);
	return ret;
}

void sort(int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	MyDB_BufferManagerPtr bufferMgr = sortMe.getBufferMgr();

	int pageNum = sortMe.getNumPages();
	queue <vector <MyDB_PageReaderWriter>> pagesList;
	vector <MyDB_RecordIteratorAltPtr> pagesIters;

	// Phase 1
	for(int i=0;i<pageNum;i++) {
		// Get it from file and sort the page
		MyDB_PageReaderWriter curPage = sortMe[i];
		curPage.sort(comparator, lhs, rhs);

		// Add the page into list for further merge
		vector<MyDB_PageReaderWriter> singoList;
		singoList.push_back(curPage);
		pagesList.push(singoList);

		// Core part of merging: when the size is full or it comes to the last pages
		if((pagesList.size() == runSize) || (i == pageNum - 1)) {
			while(pagesList.size() > 1) {
				// Get the first two vectors of pages
				vector <MyDB_PageReaderWriter> firstList = pagesList.front();
				pagesList.pop();
				vector <MyDB_PageReaderWriter> secondList = pagesList.front();
				pagesList.pop();

				// Merge
				vector <MyDB_PageReaderWriter> mergeList = mergeIntoList(bufferMgr, getIteratorAlt(firstList), getIteratorAlt(firstList), comparator, lhs, rhs);
				pagesList.push(mergeList);
			}

			// After merging finished, construct the input of mergeIntoFile
			MyDB_RecordIteratorAltPtr mergeIterList = getIteratorAlt(pagesList.front());
			pagesList.pop();
			pagesIters.push_back(mergeIterList);
		}
	}

	// Phase 2
	mergeIntoFile(sortIntoMe, pagesIters, comparator, lhs, rhs);
}

#endif
