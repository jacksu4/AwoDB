
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
        // Initialize input and output Record objects
    MyDB_RecordPtr rec = input->getEmptyRecord ();
    MyDB_RecordPtr outRec = output->getEmptyRecord ();

    // Compile for the predicate
    func pred = rec->compileComputation (selectionPredicate);

    // Get data range from the input table
    MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt();

    // Set up attributes after projection
    vector <func> finalComputations;
	for (string s : projections) {
		finalComputations.push_back (rec->compileComputation (s));
	}

    // Loop to select
    while (myIter->advance ()) {
		myIter->getCurrent (rec);

        // Check predicate result
        if(!pred ()->toBool ()) {
            continue;
        }

        // Do projection
        int i = 0;
        for (auto &f : finalComputations) {
            outRec->getAtt (i++)->set (f());
        }

        // Write
        outRec->recordContentHasChanged ();
        output->append (outRec);	
    }
}

#endif
