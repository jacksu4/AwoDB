
#ifndef REG_SELECTION_C                                        
#define REG_SELECTION_C

#include "RegularSelection.h"

RegularSelection :: RegularSelection (MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
                string selectionPredicateIn, vector <string> projectionsIn) {

	input = inputIn;
	output = outputIn;
	selectionPredicate = selectionPredicateIn;
	projections = projectionsIn;
}

void RegularSelection :: run () {

	MyDB_RecordPtr inputRec = input->getEmptyRecord ();
	MyDB_RecordPtr outputRec = output->getEmptyRecord ();
	
	// compile all of the coputations that we need here
	vector <func> finalComputations;
	for (string s : projections) {
		finalComputations.push_back (inputRec->compileComputation (s));
	}
	func pred = inputRec->compileComputation (selectionPredicate);

    // /************************************/
	// if(selectionPredicate == "bool[true]") {
	// 	MyDB_RecordPtr tempRec = input->getEmptyRecord();
	// 	MyDB_RecordIteratorAltPtr tempIter = input->getIteratorAlt();	
	// 	vector <func> computation;
	// 	for (string s : projections) {
	// 		computation.push_back (tempRec->compileComputation (s));
	// 	}
	// 	int tempSize = 0;
	// 	while(tempIter->advance()) {
	// 		tempIter->getCurrent(tempRec);
	// 		// run all of the computations
	// 		int i = 0;
	// 		for (auto &f : computation) {
	// 			cout << f();
	// 		}
	// 		cout << endl;
	// 		tempSize++;
	// 	}
	// 	cout << "input size: " << tempSize << endl;
	// }


    // /************************************/






	// now, iterate through the B+-tree query results
	MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt ();
	while (myIter->advance ()) {
		myIter->getCurrent (inputRec);

		// see if it is accepted by the predicate
		if (!pred()->toBool ()) {
			continue;
		}

		// run all of the computations
		int i = 0;
		for (auto &f : finalComputations) {
			outputRec->getAtt (i++)->set (f());
		}

		outputRec->recordContentHasChanged ();
		output->append (outputRec);
	}
}

#endif
