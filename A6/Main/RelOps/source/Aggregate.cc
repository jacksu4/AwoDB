
#ifndef AGG_CC
#define AGG_CC

#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include <unordered_map>

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

    // Initialize the schemas
	MyDB_SchemaPtr combinedSchema = make_shared <MyDB_Schema> ();
	MyDB_SchemaPtr outputSchema = make_shared <MyDB_Schema> ();
    size_t schemaSize = aggsToCompute.size() + groupings.size();
    int index = 0;
    for(auto &att : output->getTable ()->getSchema ()->getAtts ()) {
        if(index < groupings.size()) {
            combinedSchema->appendAtt(make_pair ("GroupAtt" + to_string (index), att.second));
            outputSchema->appendAtt(make_pair ("GroupAtt" + to_string (index), att.second));
        } else {
            combinedSchema->appendAtt(make_pair ("AggAtt" + to_string (index - groupings.size()), att.second));
            outputSchema->appendAtt(make_pair ("AggAtt" + to_string (index - groupings.size()), att.second));
        }
        index++;
    }
    for(auto &&att : input->getTable ()->getSchema ()->getAtts ()) {
        combinedSchema->appendAtt(att);
    }
    
    // Initialize records to be used
    MyDB_RecordPtr inputRec = input->getEmptyRecord ();
    MyDB_RecordPtr outputRec = make_shared<MyDB_Record>(outputSchema);
    MyDB_RecordPtr combinedRec = make_shared<MyDB_Record>(combinedSchema);
    combinedRec->buildFrom(outputRec, inputRec);

    // Preparation for grouping
    unordered_map <size_t, vector <void *>> groupingHash;
    vector<func> groupingHashInputFuncs;

	string sameGroup = "bool[true]";
	func sameGroupFunc;
	index = 0;
    for (auto groupAtt : groupings) {
		groupingHashInputFuncs.push_back (inputRec->compileComputation (groupAtt));

        string curClause = "== (" + groupAtt + ", [GroupAtt" + to_string (index) + "])";
		if (index == 0) {
			sameGroup = curClause;
		} else {
			sameGroup = "&& (" + curClause + ", " + sameGroup + ")";
		}
		index++;
	}
	sameGroupFunc = combinedRec->compileComputation (sameGroup);	


    // Preparation for aggregate
    vector<func> aggProcessFuncs;
    vector<func> aggEndFuncs;
    int index = 0;
    for (auto &agg : aggsToCompute) {
        if(agg.first == MyDB_AggType :: aggSum) {
		    aggProcessFuncs.push_back (combinedRec->compileComputation ("+ ([AggAtt" + to_string (index) + "], " + agg.second + ")"));
            aggEndFuncs.push_back(combinedRec->compileComputation('[AggAtt' + to_string (index) + "]"));
        } else if (MyDB_AggType :: aggAvg) {
		    aggProcessFuncs.push_back (combinedRec->compileComputation ("+ ([AggAtt" + to_string (index) + "], " + agg.second + ")"));
            aggEndFuncs.push_back(combinedRec->compileComputation('[AggAtt' + to_string (index) + "]"));
        } else {
		    aggProcessFuncs.push_back (combinedRec->compileComputation ("+ ([AggAtt" + to_string (index) + "], int[1])"));
            aggEndFuncs.push_back(combinedRec->compileComputation('[AggAtt' + to_string (index) + "]"));
        }
        index++;
	}

    // Preparation for selection
    func pred = inputRec->compileComputation (selectionPredicate);

    MyDB_RecordIteratorAltPtr myIter = getIteratorAlt (input);

    while (myIter->advance ()) {

        myIter->getCurrent (inputRec);

        // Do selection
        if (!pred ()->toBool ()) {
			continue;
		}

        // TODO
    }
}

#endif

