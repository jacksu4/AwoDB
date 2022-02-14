
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
	std::priority_queue<MyDB_RecordIteratorAltPtr, vector<MyDB_RecordIteratorAltPtr>, RecordComparator> pq(comparator);

	return;
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
	MyDB_BufferManagerPtr buffer = make_shared<MyDB_BufferManager>(sortMe.getBufferMgr()->getPageSize(), runSize, 'sortFile');

	// int pageNum = sortMe.getNumPages();
	// MyDB_RecordIteratorAltPtr leftTableIter = sortMe.getIteratorAlt(0, pageNum / 2);
	// MyDB_RecordIteratorAltPtr rightTableIter = sortMe.getIteratorAlt((pageNum / 2) + 1, pageNum - 1);

	

	// Phase 1
	// vector<MyDB_PageReaderWriter> pages = mergeIntoList(buffer, leftTableIter, rightTableIter, comparator, lhs, rhs);

	// Phase 2
	vector<MyDB_RecordIteratorAltPtr> pagesIters = getIteratorAlt(pages);
	mergeIntoFile(sortIntoMe, pagesIters, comparator, lhs, rhs);

	return;
}

#endif
