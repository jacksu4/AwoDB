#ifndef REC_COMPARATOR_H
#define REC_COMPARATOR_H

#include "MyDB_Record.h"
#include "MyDB_RecordIteratorAlt.h"
#include <iostream>
using namespace std;

class RecordComparator {

public:

	RecordIteratorAltComparator (function <bool ()> comparatorIn, MyDB_RecordIteratorAltPtr leftIterIn,  MyDB_RecordIteratorAltPtr rightIterIn) {
		comparator = comparatorIn;
		leftIter = leftIterIn;
		rightIter = rightIterIn;
	}

	bool operator () (void *lhsPtr, void *rhsPtr) {
		// lhs->fromBinary (lhsPtr);
		// rhs->fromBinary (rhsPtr);
		// return comparator ();
	}

private:

	function <bool ()> comparator;
	MyDB_RecordIteratorAltPtr leftIter;
	MyDB_RecordIteratorAltPtr rightIter;

};

#endif
