
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {

    vector<MyDB_PageReaderWriter> pages;
    discoverPages(rootLocation, pages, low, high);
    MyDB_RecordPtr lhs = getEmptyRecord(), rhs = getEmptyRecord(), tmp = getEmptyRecord();
    MyDB_INRecordPtr inLhs = getINRecord(), inRhs = getINRecord();
    inLhs->setKey(low);
    inRhs->setKey(high);

    function<bool ()> comp = buildComparator(lhs, rhs), lowComp = buildComparator(tmp, inLhs), highComp = buildComparator(inRhs, tmp);

    return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pages, lhs, rhs, comp, tmp, lowComp, highComp, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {

    vector<MyDB_PageReaderWriter> pages;
    discoverPages(rootLocation, pages, low, high);
    MyDB_RecordPtr lhs = getEmptyRecord(), rhs = getEmptyRecord(), tmp = getEmptyRecord();
    MyDB_INRecordPtr inLhs = getINRecord(), inRhs = getINRecord();
    inLhs->setKey(low);
    inRhs->setKey(high);

    function<bool ()> comp = buildComparator(lhs, rhs), lowComp = buildComparator(tmp, inLhs), highComp = buildComparator(inRhs, tmp);

    return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pages, lhs, rhs, comp, tmp, lowComp, highComp, false);
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &list, MyDB_AttValPtr low, MyDB_AttValPtr high) {
	MyDB_PageReaderWriter searchPage = (*this)[whichPage];

    if (searchPage.getType() == RegularPage) {
        list.push_back(searchPage);
        return true;
    }
    MyDB_INRecordPtr lowRec = getINRecord(), highRec = getINRecord(), curRec = getINRecord();
    lowRec->setKey(low);
    highRec->setKey(high);
    function<bool ()> curIsLowerThanLowRec = buildComparator(curRec, lowRec), curIsHigherThanHighRec = buildComparator(highRec, curRec);

    MyDB_RecordIteratorAltPtr iterator = searchPage.getIteratorAlt();
    
    while(iterator->advance()) {
        iterator->getCurrent(curRec);
        if(!curIsLowerThanLowRec()) { //within the low bound, continue
            discoverPages(curRec->getPtr(), list, low, high);
        }
        if(curIsHigherThanHighRec()) {
            break;
        }
    }
    return false;
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {
    if (getNumPages() <= 1) { // empty B+ tree
        MyDB_PageReaderWriter root = (*this)[++rootLocation]; // Set an Internal node root page
        root.setType(DirectoryPage);
        MyDB_INRecordPtr internalNode = getINRecord();

        int pageLoc = getTable()->lastPage() + 1;
        getTable()->setLastPage(pageLoc);

        MyDB_PageReaderWriter leaf = (*this)[pageLoc]; // Set a leaf page
        leaf.clear();
        leaf.setType(RegularPage);
        internalNode->setPtr(pageLoc);
        root.append(internalNode); // Append the INRecord with the leaf page ptr to the root page
    }
    MyDB_RecordPtr splitRes = append(rootLocation, appendMe);
    if (splitRes) { // If there's a split in root
        int newRoot = getTable()->lastPage() + 1; // set the new Root
        getTable()->setLastPage(newRoot);
        MyDB_PageReaderWriter newPage = (*this)[newRoot];
        newPage.clear();
        newPage.setType(DirectoryPage);
        newPage.append(splitRes); // set two ptrs on new root. One toward the new split INRecord, one toward the old root.
        MyDB_INRecordPtr oldRoot = getINRecord();
        oldRoot->setPtr(rootLocation);
        newPage.append(oldRoot);
        rootLocation = newRoot;
    }
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
    int idx = getTable()->lastPage() + 1;
    getTable()->setLastPage(idx);
    MyDB_PageReaderWriter pageOne = (*this)[idx];
    pageOne.clear();

    int secIdx = getTable()->lastPage() + 1; //used for temporary storage
    MyDB_PageReaderWriter pageTwo = (*this)[secIdx];
    pageTwo.clear();

    MyDB_INRecordPtr recordPtr = getINRecord();
    recordPtr->setPtr(idx);

    MyDB_RecordPtr lhs, rhs, tmpRecord;
    MyDB_PageType pageType = splitMe.getType();

    if (pageType == RegularPage) {
        lhs = getEmptyRecord(), rhs = getEmptyRecord(), tmpRecord = getEmptyRecord();
        pageOne.setType(RegularPage);
        pageTwo.setType(RegularPage);
    } else {
        lhs = getINRecord(), rhs = getINRecord(), tmpRecord = getINRecord();
        pageOne.setType(DirectoryPage);
        pageTwo.setType(DirectoryPage);
    }
    function<bool()> comparator = buildComparator(lhs, rhs);
    splitMe.sortInPlace(comparator, lhs, rhs);

    MyDB_RecordIteratorAltPtr iterator = splitMe.getIteratorAlt();
    int sizeOfSplitMe = 0;

    while (iterator->advance()) { // get the size
        iterator->getCurrent(tmpRecord);
        sizeOfSplitMe++;
    }

    if (pageType == RegularPage) {
        tmpRecord = getEmptyRecord();
    } else {
        tmpRecord = getINRecord();
    }

    int median = sizeOfSplitMe / 2, count = 0;
    iterator = splitMe.getIteratorAlt();

    while (iterator->advance()) {
        iterator->getCurrent(tmpRecord);
        if (count == median) {
            recordPtr->setKey(getKey(tmpRecord));
        }
        if (count <= median) {
            pageOne.append(tmpRecord);
        } else {
            pageTwo.append(tmpRecord);
        }
        count++;
    }

    auto andMeInNewPage = buildComparator(andMe, recordPtr);
    if (andMeInNewPage()) {
        pageOne.append(andMe);
        pageOne.sortInPlace(comparator, lhs, rhs);
    } else {
        pageTwo.append(andMe);
        pageTwo.sortInPlace(comparator, lhs, rhs);
    }

    splitMe.clear();
    if (pageType == DirectoryPage) {
        splitMe.setType(DirectoryPage);
    }
    iterator = pageTwo.getIteratorAlt();
    if (pageType == RegularPage) {
        tmpRecord = getEmptyRecord();
    } else {
        tmpRecord = getINRecord();
    }

    while (iterator->advance()) {
        iterator->getCurrent(tmpRecord);
        splitMe.append(tmpRecord);
    }
    pageTwo.clear();
    return recordPtr;

}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe) {
    MyDB_PageReaderWriter page = (*this)[whichPage];
    if (!appendMe->getSchema() || page.getType() == RegularPage) { // Internal node record case or Regular page with regular record appendMe
        if (page.append(appendMe)) {
            if (!appendMe->getSchema()) { // If Internal node case, need to sort based on the proj description
                MyDB_INRecordPtr lhs = getINRecord(), rhs = getINRecord();
                auto myComparator = buildComparator(lhs, rhs);
                page.sortInPlace(myComparator, lhs, rhs);
            }
            return nullptr;
        } else {
            return split(page, appendMe);
        }
    } else { // Internal node page with regular record
        MyDB_RecordIteratorAltPtr recordIter = page.getIteratorAlt();
        MyDB_INRecordPtr record = getINRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            auto myComparator = buildComparator(appendMe, record);
            if (myComparator()) {
                auto res = append(record->getPtr(), appendMe); // Recursively find the appropriate node
                if (res) {
                    return append(whichPage, res); // if there is return ptr which means split happens, append that to the current record
                }
                return nullptr;
            }
        }
    }
    return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
    printHelper(this->rootLocation, 0);
}

void MyDB_BPlusTreeReaderWriter::printHelper(int whichPage, int depth) {
    MyDB_PageReaderWriter page = (*this)[whichPage];
    MyDB_RecordIteratorAltPtr recordIter = page.getIteratorAlt();

    // leaf
    if (page.getType() == MyDB_PageType::RegularPage) {
        MyDB_RecordPtr record = getEmptyRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            cout << depth << "\n";
            cout << record << "\n";
        }
        cout << "\n";
    }
        // inner node
    else {
        MyDB_INRecordPtr inRecord = getINRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(inRecord);
            printHelper(inRecord->getPtr(), depth + 1);
            cout << depth << "\n";
            cout << inRecord->getKey() << "\n";
        }
        cout << "\n";
    }
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}


#endif
