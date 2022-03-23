
#ifndef AGG_CC
#define AGG_CC

#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include <unordered_map>
#include <iostream>

using namespace std;

Aggregate :: Aggregate (MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
		vector <pair <MyDB_AggType, string>> aggsToComputeIn,
		vector <string> groupingsIn, string selectionPredicateIn) {
            input = inputIn;
            output = outputIn;
            aggsToCompute = aggsToComputeIn;
            groupings = groupingsIn;
            selectionPredicate = selectionPredicateIn;
        }

void Aggregate :: run () {

	// cout << "Aggregate: Initialize the schemas\n" << flush;
    // Initialize the schemas
	MyDB_SchemaPtr combinedSchema = make_shared <MyDB_Schema> ();
	MyDB_SchemaPtr aggSchema = make_shared <MyDB_Schema> ();
    size_t schemaSize = aggsToCompute.size() + groupings.size();
    int index = 0;
	for(auto &&att : input->getTable ()->getSchema ()->getAtts ()) {
        combinedSchema->appendAtt(att);
    }
    for(auto &att : output->getTable ()->getSchema ()->getAtts ()) {
        if(index < groupings.size()) {
            combinedSchema->appendAtt(make_pair ("GroupAtt" + to_string (index), att.second));
            aggSchema->appendAtt(make_pair ("GroupAtt" + to_string (index), att.second));
        } else {
            combinedSchema->appendAtt(make_pair ("AggAtt" + to_string (index - groupings.size()), att.second));
            aggSchema->appendAtt(make_pair ("AggAtt" + to_string (index - groupings.size()), att.second));
        }
        index++;
    }
	combinedSchema->appendAtt(make_pair ("Count", make_shared<MyDB_IntAttType>()));
	aggSchema->appendAtt(make_pair ("Count", make_shared<MyDB_IntAttType>()));
    
	// cout << "Aggregate: Initialize records to be used\n" << flush;
    // Initialize records to be used
    MyDB_RecordPtr inputRec = input->getEmptyRecord ();
    MyDB_RecordPtr aggRec = make_shared<MyDB_Record>(aggSchema);
    MyDB_RecordPtr combinedRec = make_shared<MyDB_Record>(combinedSchema);
    combinedRec->buildFrom(inputRec, aggRec);

	// cout << "Aggregate: Preparation for grouping\n" << flush;
    // Preparation for grouping
    unordered_map <size_t, vector <void *>> groupingHash;
    vector<func> groupingFuncs;

	string sameGroup = "bool[true]";
	func sameGroupFunc;
	index = 0;
    for (auto groupAtt : groupings) {
		groupingFuncs.push_back (inputRec->compileComputation (groupAtt));

        string curClause = "== (" + groupAtt + ", [GroupAtt" + to_string (index) + "])";
		if (index == 0) {
			sameGroup = curClause;
		} else {
			sameGroup = "&& (" + curClause + ", " + sameGroup + ")";
		}
		index++;
	}
	sameGroupFunc = combinedRec->compileComputation (sameGroup);	

	// cout << "Aggregate: Preparation for aggregate\n" << flush;
    // Preparation for aggregate
    vector<func> aggProcessFuncs;
    vector<func> aggEndFuncs;
    index = 0;
    for (auto &agg : aggsToCompute) {
		// cout << agg.first << "\n" << flush;
        if(agg.first == MyDB_AggType :: sum) {
		    aggProcessFuncs.push_back (combinedRec->compileComputation ("+ ([AggAtt" + to_string (index) + "], " + agg.second + ")"));
            aggEndFuncs.push_back(combinedRec->compileComputation("[AggAtt" + to_string (index) + "]"));
        } else if (agg.first == MyDB_AggType :: avg) {
		    aggProcessFuncs.push_back (combinedRec->compileComputation ("+ ([AggAtt" + to_string (index) + "], " + agg.second + ")"));
            aggEndFuncs.push_back(combinedRec->compileComputation("/ ([AggAtt" + to_string (index) + "], [Count])"));
        } else {
		    aggProcessFuncs.push_back (combinedRec->compileComputation ("+ ([AggAtt" + to_string (index) + "], int[1])"));
            aggEndFuncs.push_back(combinedRec->compileComputation("[AggAtt" + to_string (index) + "]"));
        }
        index++;
	}
	aggProcessFuncs.push_back (combinedRec->compileComputation ("+ ( int[1], [Count])"));
	aggEndFuncs.push_back (combinedRec->compileComputation ("[Count]"));

	// cout << "Aggregate: Preparation for selection\n" << flush;
    // Preparation for selection
    func pred = inputRec->compileComputation (selectionPredicate);

	// cout << "Aggregate: Preparation for temp data using input table's buffer manager\n" << flush;
	// Preparation for temp data using input table's buffer manager
	MyDB_PageReaderWriterPtr lastPage = make_shared<MyDB_PageReaderWriter>(true, *(input->getBufferMgr ()));
	vector<MyDB_PageReaderWriter> tempPages;
	tempPages.push_back(*lastPage);

	// cout << "Aggregate: Start iteration!\n" << flush;
    // Start iteration
    MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt ();
    while (myIter->advance ()) {

        myIter->getCurrent (inputRec);

		// cout << "Aggregate Iteration: Do Selection\n" << flush;
        // Do selection
        if (!pred ()->toBool ()) {
			continue;
		}

		// cout << "Aggregate Iteration: match aggRec, Hashing\n" << flush;
        size_t hashVal = 0;
		for (auto &f : groupingFuncs) {
			hashVal ^= f ()->hash ();
		}

		// cout << "Aggregate Iteration: match aggRec, checking\n" << flush;
		// Find the corresponding aggRec
		vector <void *> &possiblePos = groupingHash[hashVal];
		cout << possiblePos.size() << "\n" << flush;
		void * loc = nullptr;
		for(auto &pos : possiblePos) {
			aggRec->fromBinary(pos);
			if(!sameGroupFunc ()->toBool ()) {
				continue;
			}
			loc = pos;
			break;
		}

		// cout << "Aggregate Iteration: Not match, Initializing new record\n" << flush;
		if(loc == nullptr) {
			int i = 0;
			for (auto &f : groupingFuncs) {
				aggRec->getAtt (i++)->set (f ());
			}
			for (int j = 0; j < aggProcessFuncs.size (); j++) {
				aggRec->getAtt (i++)->set (make_shared<MyDB_IntAttVal>());
			}
		}

		// cout << "Aggregate Iteration: calculate current result\n" << flush;
		// Update aggregation result
		int i = 0;
		for(auto &f : aggProcessFuncs) {
			aggRec->getAtt (groupings.size() + i++)->set (f ());
		}
		aggRec->recordContentHasChanged ();

		// cout << "Aggregate Iteration: match, write it\n" << flush;
		if(loc != nullptr) {
			aggRec->toBinary (loc);
			continue;
		}

		// cout << "Aggregate Iteration: Not match, generate it in buffer\n" << flush;
		while(loc == nullptr) {
			loc = lastPage->appendAndReturnLocation(aggRec);
			if(loc == nullptr) {
				lastPage = make_shared<MyDB_PageReaderWriter>(true, *(input->getBufferMgr ()));
				tempPages.push_back(*lastPage);
			} else {
				possiblePos.push_back(loc);
			}
		}
    }

	// cout << "Aggregate: Output\n" << flush;
	// Output
	MyDB_RecordPtr outputRec = output->getEmptyRecord ();
	MyDB_RecordIteratorAltPtr ouputIter = getIteratorAlt (tempPages);
	while(ouputIter->advance ()) {
		ouputIter->getCurrent (aggRec);
		index = 0;
		while(index < groupings.size()) {
			outputRec->getAtt (index)->set (aggRec->getAtt (index));
			index++;
		}
		index = 0;
		for(auto &f : aggEndFuncs) {
			if(index >= aggsToCompute.size()) {
				break;
			}
			outputRec->getAtt (index + groupings.size())->set (f ());
			index++;
		}
		outputRec->recordContentHasChanged ();
		output->append (outputRec);	
	}
}

#endif

