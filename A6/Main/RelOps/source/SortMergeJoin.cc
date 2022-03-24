
#ifndef SORTMERGE_CC
#define SORTMERGE_CC

#include <functional>
#include "Aggregate.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "Sorting.h"
#include <iostream>

SortMergeJoin :: SortMergeJoin (MyDB_TableReaderWriterPtr leftInputIn, MyDB_TableReaderWriterPtr rightInputIn,
		MyDB_TableReaderWriterPtr outputIn, string finalSelectionPredicateIn, 
		vector <string> projectionsIn,
		pair <string, string> equalityCheckIn, string leftSelectionPredicateIn,
		string rightSelectionPredicateIn) {

            output = outputIn;
            finalSelectionPredicate = finalSelectionPredicateIn;
            projections = projectionsIn;
            if(leftInputIn->getNumPages () < rightInputIn->getNumPages ()) {
                equalityCheck = equalityCheckIn;
                leftInput = leftInputIn;
                rightInput = rightInputIn;
                leftSelectionPredicate = leftSelectionPredicateIn;
                rightSelectionPredicate = rightSelectionPredicateIn;
            } else {
                equalityCheck = make_pair (equalityCheckIn.second, equalityCheckIn.first);
                leftInput = rightInputIn;
                rightInput = leftInputIn;
                leftSelectionPredicate = rightSelectionPredicateIn;
                rightSelectionPredicate = leftSelectionPredicateIn;
            }
            runSize = leftInput->getBufferMgr ()->numPages / 2;
        }

void SortMergeJoin :: run () {
    
    //
    // PREPARATION
    //

    // Initialize record objects 
    MyDB_RecordPtr leftRec = leftInput->getEmptyRecord ();
    MyDB_RecordPtr leftHelpRec = leftInput->getEmptyRecord ();

    MyDB_RecordPtr rightRec = rightInput->getEmptyRecord ();
    MyDB_RecordPtr rightHelpRec = rightInput->getEmptyRecord ();

    MyDB_RecordPtr outputRec = output->getEmptyRecord ();
    
    // Initialize combined record object for final selection and projection
	MyDB_SchemaPtr combinedSchema = make_shared <MyDB_Schema> ();
	for(auto &&att : leftInput->getTable ()->getSchema ()->getAtts ()) {
        combinedSchema->appendAtt(att);
    }
    for(auto &&att : rightInput->getTable ()->getSchema ()->getAtts ()) {
        combinedSchema->appendAtt(att);
    }
    MyDB_RecordPtr combinedRec = make_shared<MyDB_Record> (combinedSchema);
    combinedRec->buildFrom (leftRec, rightRec);

    // Initialize seperate selection functions of the two tables
    func leftPred = leftRec->compileComputation (leftSelectionPredicate);
    func rightPred = rightRec->compileComputation (rightSelectionPredicate);
    function <bool ()> leftComp = buildRecordComparator (leftRec, leftHelpRec, equalityCheck.first);
    function <bool ()> rightComp = buildRecordComparator (rightRec, rightHelpRec, equalityCheck.second);
    // funcion<bool> comp = buildRecordComparator (leftRec, rightRec, equalityCheck.first, equalityCheck.second);

	// Set up comparison of keys in current records of two tables
	func leftSmaller = buildRecordComparator(leftRec,  rightRec, equalityCheck.first, equalityCheck.second, -1);
	func leftLarger = buildRecordComparator(leftRec,  rightRec, equalityCheck.first, equalityCheck.second, 1);
	func leftEqual = buildRecordComparator(leftRec,  rightRec, equalityCheck.first, equalityCheck.second, 0);

    // Initialize final selection function
    func finalPred = combinedRec->compileComputation (finalSelectionPredicate);

    // Initialize final projection functions
	vector <func> finalProjection;
	for (string s : projections) {
		finalProjection.push_back (combinedRec->compileComputation (s));
	}
    
    // Delare the iterator and storage of temporary left records for join
	MyDB_PageReaderWriter lastPage (true, *(leftInput->getBufferMgr ()));
	vector<MyDB_PageReaderWriter> tempPages;
    MyDB_RecordIteratorAltPtr leftInnerIter;

    //
    // ACTION
    //

    // Sort and do the first selection for the two tables
    MyDB_RecordIteratorAltPtr leftIter = buildItertorOverSortedRuns (runSize, *leftInput, leftComp, 
                leftRec, leftHelpRec, leftSelectionPredicate);
    MyDB_RecordIteratorAltPtr rightIter = buildItertorOverSortedRuns (runSize, *rightInput, rightComp, 
                rightRec, rightHelpRec, rightSelectionPredicate);

	if (!leftIter->advance () || !rightIter->advance ()) {
		return;
    }
    leftIter->getCurrent (leftRec);
    rightIter->getCurrent (rightRec);
    bool flag = true;
    while(flag) {
        if(leftSmaller ()->toBool ()) {
            if(!leftIter->advance ()) {
                flag = false;
                break;
            }
            leftIter->getCurrent (leftRec);
        } else if(leftLarger ()->toBool ()) {
            if(!rightIter->advance ()) {
                flag = false;
                break;
            }
            rightIter->getCurrent (rightRec);
        } else if(leftEqual ()->toBool ()) {
            tempPages.clear ();
            lastPage.clear ();

            tempPages.push_back(lastPage);
            
            leftIter->getCurrent (leftRec);
            leftIter->getCurrent (leftHelpRec);
            
            // Iterate records in left table to find the pages with the same key
            while(flag) {
                if(!lastPage.append(leftRec)) {
                    MyDB_PageReaderWriter nextPage (true, *(leftInput->getBufferMgr ()));
                    lastPage = nextPage;
                    tempPages.push_back(lastPage);
                    continue;
                }

                if(!leftIter->advance ()) {
                    flag = false;
                    break;
                }
                
                leftIter->getCurrent (leftHelpRec);
                if(!leftComp ()) {
                    leftIter->getCurrent (leftRec);
                } else {
                    break;
                }
            }

            // Iterate records in right table
            rightIter->getCurrent (rightRec);
            while(leftEqual ()->toBool ()) {
                leftInnerIter = getIteratorAlt (tempPages);

                while(leftInnerIter->advance ()) {
                    leftInnerIter->getCurrent (leftRec);
                    if(!finalPred ()->toBool ()) {
                        continue;
                    }
                    int i = 0;
                    for (auto &f : finalProjection) {
                        outputRec->getAtt (i++)->set (f ());
                    }
                    outputRec->recordContentHasChanged ();
                    output->append (outputRec);	
                }

                if(!rightIter->advance ()) {
                    flag = false;
                    break;
                }
                rightIter->getCurrent (rightRec);
            }

            // cout << "Iterate Equal: update\n" << flush;
            if(flag) {
                leftIter->getCurrent (leftRec);
                rightIter->getCurrent (rightRec);
            }
        }
    }
}

#endif
