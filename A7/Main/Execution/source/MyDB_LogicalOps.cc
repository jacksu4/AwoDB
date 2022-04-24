
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include <sstream>
#include "MyDB_LogicalOps.h"
#include "RegularSelection.h"
#include "Aggregate.h"
#include <time.h>
#include <unordered_set>
#include <typeinfo>

// fill this out!  This should actually run the aggregation via an appropriate RelOp, and then it is going to
// have to unscramble the output attributes and compute exprsToCompute using an execution of the RegularSelection 
// operation (why?  Note that the aggregate always outputs all of the grouping atts followed by the agg atts.
// After, a selection is required to compute the final set of aggregate expressions)
//
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two 
// sides should be deleted (via a kill to killFile () on the buffer manager)
MyDB_TableReaderWriterPtr LogicalAggregate :: execute () {
//    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, mgr);
//    string groupingString;
//    if (groupings.size() == 1) {
//        groupingString = groupings[0]->toString();
//    } else {
//        groupingString = groupings[0]->toString();
//        for (int i = 1; i < groupings.size(); i++) {
//            groupingString = "&& (" + groupingString + ", " + groupings[i]->toString() + ")";
//        }
//    }
//
//    cout << groupingString << endl;
//
//    Aggregate agg(inputOp, outputTable, exprsToCompute, groupingString, );
//    agg.run();
//
//    MyDB_RecordPtr rec = outputTable->getEmptyRecord();
//    MyDB_RecordIteratorAltPtr iter = outputTable->getIteratorAlt();
//    int size = 0;
//    while(iter->advance()) {
//        iter->getCurrent(rec);
//        if(size < 30) {
//            cout << rec << endl;
//        }
//        size++;
//    }
//    if(remove("topStorageLoc") != 0) {
//        perror("Error deleting file");
//    } else {
//        puts("File successfully deleted");
//    }
    time_t start, stop;
    start = time(NULL);
    MyDB_TableReaderWriterPtr inputTable = inputOp->execute(); 

    MyDB_SchemaPtr aggTempSchema = make_shared<MyDB_Schema>();

    vector <bool> isAggList;
    vector <string> exprStrings;
    vector <pair<string, MyDB_AttTypePtr>> groupingList;
    vector <pair<string, MyDB_AttTypePtr>>  aggList;
    vector <string> groupingsStrings;
    string selectionPredString = "bool[true]";
    string reorderSelectionPred = "bool[true]";
    int countSelectionPred = 0;
    vector <pair <MyDB_AggType, string>> aggsToCompute;
    vector <string> exprsForReorder;
    

    int index = 0;
    for (auto b: outputSpec->getSchema ()->getAtts ()) {
        ExprTreePtr curExpr = exprsToCompute[index];
        string exprString = curExpr->toString();
        exprsForReorder.push_back("[" + b.first + "]");

        if(curExpr->hasAgg()) {
            if(curExpr->isAvg()) {
                aggList.push_back(make_pair (b.first, make_shared <MyDB_DoubleAttType> ()));
                aggsToCompute.push_back(make_pair (MyDB_AggType::aggAvg, exprString.substr(4, exprString.size() - 5)));
            } else {
                aggList.push_back(make_pair (b.first, b.second));
                aggsToCompute.push_back(make_pair (MyDB_AggType::aggSum, exprString.substr(4, exprString.size() - 5)));
            }
        } else {
            groupingList.push_back(make_pair (b.first, b.second));
            groupingsStrings.push_back(exprString);
        }
        
        index++;
    }
    for(auto g: groupingList) {
        aggTempSchema->appendAtt(g);
    }
    for(auto a: aggList) {
        aggTempSchema->appendAtt(a);
    }
    
    MyDB_TableReaderWriterPtr aggOutputTable = make_shared<MyDB_TableReaderWriter>(make_shared<MyDB_Table> ("table", "aggTemp", aggTempSchema), make_shared <MyDB_BufferManager> (131072, 4028, "tempAggFile"));
    
    Aggregate aggregate (inputTable, aggOutputTable, aggsToCompute, groupingsStrings, selectionPredString);
    aggregate.run();

    MyDB_BufferManagerPtr inputMgr = inputTable->getBufferMgr();
    inputMgr->killTable(inputTable->getTable());

    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, make_shared <MyDB_BufferManager> (131072, 4028, "AggFile"));

    RegularSelection selection (aggOutputTable, outputTable, reorderSelectionPred, exprsForReorder);
    selection.run();

    MyDB_BufferManagerPtr aggTempMgr = aggOutputTable->getBufferMgr();
    aggTempMgr->killTable(aggOutputTable->getTable());

    MyDB_RecordPtr rec = outputTable->getEmptyRecord();
    MyDB_RecordIteratorAltPtr iter = outputTable->getIteratorAlt();
    int size = 0;
    while(iter->advance()) {
        iter->getCurrent(rec);
        if(size < 30) {
            cout << rec << endl;
        }
        size++;
    }

    if (size >= 30) {
        cout << "Finished printing first 30 records" << endl;
    }

    stop = time(NULL);
    printf("Time used is %ld seconds.\n", (stop - start));
    printf("The number of total records is %d.\n", size);

    remove("AggStorageLoc");

	return outputTable;

}
// we don't really count the cost of the aggregate, so cost its subplan and return that
pair <double, MyDB_StatsPtr> LogicalAggregate :: cost () {
	return inputOp->cost ();
}
	
// this costs the entire query plan with the join at the top, returning the compute set of statistics for
// the output.  Note that it recursively costs the left and then the right, before using the statistics from
// the left and the right to cost the join itself
pair <double, MyDB_StatsPtr> LogicalJoin :: cost () {
	auto left = leftInputOp->cost ();
	auto right = rightInputOp->cost ();
	MyDB_StatsPtr outputStats = left.second->costJoin (outputSelectionPredicate, right.second);
	return make_pair (left.first + right.first + outputStats->getTupleCount (), outputStats);
}
	
// Fill this out!  This should recursively execute the left hand side, and then the right hand side, and then
// it should heuristically choose whether to do a scan join or a sort-merge join (if it chooses a scan join, it
// should use a heuristic to choose which input is to be hashed and which is to be scanned), and execute the join.
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two 
// sides should be deleted (via a kill to killFile () on the buffer manager)
MyDB_TableReaderWriterPtr LogicalJoin :: execute () {
//    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, mgr);
//    string selectionPredString;
//    if (outputSelectionPredicate.size() == 1) {
//        selectionPredString = outputSelectionPredicate[0]->toString();
//    } else {
//        selectionPredString = "&& (" + outputSelectionPredicate[0]->toString() + ", " + outputSelectionPredicate[1]->toString() + ")";
//        for (int i = 2; i < outputSelectionPredicate.size(); i++) {
//            selectionPredString = "&& (" + selectionPredString + ", " + outputSelectionPredicate[i]->toString() + ")";
//        }
//    }
	return nullptr;
}

// this costs the table scan returning the compute set of statistics for the output
pair <double, MyDB_StatsPtr> LogicalTableScan :: cost () {
	MyDB_StatsPtr returnVal = inputStats->costSelection (selectionPred);
	return make_pair (returnVal->getTupleCount (), returnVal);	
}

string LogicalTableScan :: cutPrefix(string input, string alias) {
    size_t start_pos = input.find("["+alias+"_"+alias+"_");
    while (start_pos != std::string::npos) {
        input = input.substr(0, start_pos) + "[" + alias + "_" + input.substr(start_pos+3+alias.length()*2);
        start_pos = input.find("["+alias+"_"+alias+"_");
    }
    return input;
}

// fill this out!  This should heuristically choose whether to use a B+-Tree (if appropriate) or just a regular
// table scan, and then execute the table scan using a relational selection.  Note that a desirable optimization
// is to somehow set things up so that if a B+-Tree is NOT used, that the table scan does not actually do anything,
// and the selection predicate is handled at the level of the parent (by filtering, for example, the data that is
// input into a join)
MyDB_TableReaderWriterPtr LogicalTableScan :: execute () {
    
    time_t start, stop;
    start = time(NULL);
    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, make_shared <MyDB_BufferManager> (131072, 4028, "tempFile"));
    string selectionPredString;
    if (selectionPred.size() == 1) {
        selectionPredString = cutPrefix(selectionPred[0]->toString(), aliasName);
    } else {
        selectionPredString = cutPrefix(selectionPred[0]->toString(), aliasName);
        for (int i = 1; i < selectionPred.size(); i++) {
            selectionPredString = "&& (" + selectionPredString + ", " + cutPrefix(selectionPred[i]->toString(), aliasName) + ")";
        }
    }

    RegularSelection selection (inputSpec, outputTable, selectionPredString, exprsToCompute);
    selection.run();

    MyDB_RecordPtr rec = outputTable->getEmptyRecord();
    MyDB_RecordIteratorAltPtr iter = outputTable->getIteratorAlt();
    int size = 0;
    while(iter->advance()) {
        iter->getCurrent(rec);
        if(size < 30) {
            cout << rec << endl;
        }
        size++;
    }

    if (size >= 30) {
        cout << "Finished printing first 30 records" << endl;
    }

    stop = time(NULL);
    printf("Time used is %ld seconds.\n", (stop - start));
    printf("The number of total records is %d.\n", size);

    remove("storageLoc");

	return outputTable;
}

#endif
