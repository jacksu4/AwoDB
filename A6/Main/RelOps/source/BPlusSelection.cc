
#ifndef BPLUS_SELECTION_C                                        
#define BPLUS_SELECTION_C

#include "BPlusSelection.h"

BPlusSelection :: BPlusSelection (MyDB_BPlusTreeReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
                MyDB_AttValPtr low, MyDB_AttValPtr high,
                string selectionPredicateIn, vector <string> projectionsIn) {
                    input = inputIn;
                    output = outputIn;
                    low = lowIn;
                    high = highIn;
                    selectionPredicate = selectionPredicateIn;
                    projections = projectionsIn;

                }

void BPlusSelection :: run () {
    
    // Initialize input and output Record objects
    MyDB_RecordPtr rec = input.getEmptyRecord ();
    MyDB_RecordPtr outRec = output.getEmptyRecord ();

    // Compile for the predicate
    func pred = rec->compileComputation (selectionPredicate);

    // Get data range from the input table
    MyDB_RecordIteratorAltPtr myIter = input.getSortedRangeIteratorAlt(low, high);

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
        output->append (outputRec);	
    }
}

#endif
