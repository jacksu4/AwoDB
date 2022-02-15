#ifndef REC_ITERCOMPARATOR_H
#define REC_ITERCOMPARATOR_H

#include "MyDB_Record.h"
#include "MyDB_RecordIteratorAlt.h"
#include <iostream>
using namespace std;

class RecordIteratorAltComparator {

public:

	RecordIteratorAltComparator (function <bool ()> comparatorIn, MyDB_RecordPtr lhsIn, MyDB_RecordPtr rhsIn) {
		comparator = comparatorIn;
		lhs = lhsIn;
		rhs = rhsIn;
	}

	bool operator () (MyDB_RecordIteratorAltPtr leftIter, MyDB_RecordIteratorAltPtr rightIter) {
		leftIter->getCurrent(lhs);
		rightIter->getCurrent(rhs);
		return !comparator ();
	}

private:

	function <bool ()> comparator;
	MyDB_RecordPtr lhs;
	MyDB_RecordPtr rhs;

};

#endif
