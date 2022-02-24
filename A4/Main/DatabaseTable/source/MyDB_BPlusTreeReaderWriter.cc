
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
    } else {
        MyDB_INRecordPtr lowRec = getINRecord(), highRec = getINRecord(), curRec = getINRecord();
        function<bool ()> curIsLowerThanLowRec = buildComparator(curRec, lowRec), curIsHigherThanHighRec = buildComparator(highRec, curRec);
        lowRec->setKey(low);
        highRec->setKey(high);

        MyDB_RecordIteratorAltPtr iterator = searchPage.getIteratorAlt();

        bool leaf = false;
        while(iterator->advance()) {
            iterator->getCurrent(curRec);
            if(!curIsHigherThanHighRec && !curIsLowerThanLowRec) { //within the high and low bound, continue
                if (leaf) {
                    list.push_back((*this)[curRec->getPtr()]);
                } else {
                    leaf = discoverPages(curRec->getPtr(), list, low, high);
                }
            }
        }
        return false;
    }
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {
    if (rootLocation == -1) { // empty B+ tree
        auto root = (*this)[++rootLocation];
        root.setType(DirectoryPage);
        root.clear();
        MyDB_INRecordPtr internalNode = getINRecord();
        internalNode->setPtr(1);
        getTable()->setLastPage(1);

        root.clear();
        root.append(internalNode);

        MyDB_PageReaderWriter leaf = (*this)[1];
        leaf.setType(RegularPage);
        leaf.clear();
        leaf.append(appendMe);
    } else {
        auto recPtr = append(rootLocation, appendMe);
        if(recPtr) {
            size_t newLoc = getTable()->lastPage() + 1;
            getTable()->setLastPage(newLoc);
            MyDB_PageReaderWriter newRoot = (*this)[newLoc];
            newRoot.setType(DirectoryPage);
            newRoot.clear();

            auto tmp = getINRecord();
            tmp->setPtr(rootLocation);
            newRoot.append(recPtr);
            newRoot.append(tmp);
            rootLocation = newLoc;
            getTable()->setRootLocation(rootLocation);
        }
    }
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
    int idx = getTable()->lastPage() + 1;
    getTable() ->setLastPage(idx);
    MyDB_PageReaderWriter pageOne = (*this)[idx];

    int secIdx = getTable()->lastPage() + 1; //used for temporary storage
    getTable() ->setLastPage(secIdx);
    MyDB_PageReaderWriter pageTwo = (*this)[secIdx];

    MyDB_INRecordPtr recordPtr = getINRecord();
    recordPtr->setPtr(idx);

    MyDB_RecordPtr lhs, rhs;
    MyDB_PageType pageType;
    MyDB_RecordPtr tmpRecord;

    if (splitMe.getType() == RegularPage) {
        lhs = getEmptyRecord(), rhs = getEmptyRecord(), tmpRecord = getEmptyRecord();;
        pageType = RegularPage;
    } else {
        lhs = getINRecord(), rhs = getINRecord(), tmpRecord = getINRecord();;
        pageType = DirectoryPage;
    }
    function<bool ()> comparator = buildComparator(lhs, rhs);
    splitMe.sortInPlace(comparator, lhs, rhs);

    MyDB_RecordIteratorAltPtr iterator = splitMe.getIteratorAlt();
    size_t sizeOfSplitMe = 0;

    while(iterator->advance()) { // get the size
        iterator->getCurrent(tmpRecord);
        sizeOfSplitMe++;
    }

    if(pageType == RegularPage) {
        tmpRecord = getEmptyRecord();
    } else {
        tmpRecord = getINRecord();
    }

    size_t median = sizeOfSplitMe / 2 + 1, count = 0;
    iterator = splitMe.getIteratorAlt();

    while(iterator->advance()) {
        iterator->getCurrent(tmpRecord);
        if(count == median) {
            recordPtr->setKey(getKey(tmpRecord));
        }
        if(count <= median) {
            pageOne.append(tmpRecord);
        } else {
            pageTwo.append(tmpRecord);
        }
        count++;
    }

    auto andMeInNewPage = buildComparator(andMe, recordPtr);
    if(andMeInNewPage) {
        pageOne.append(andMe);
        pageOne.sortInPlace(comparator, lhs, rhs);
    } else {
        pageTwo.append(andMe);
        pageTwo.sortInPlace(comparator, lhs, rhs);
    }

    iterator = pageTwo.getIteratorAlt();
    tmpRecord = getEmptyRecord();
    splitMe.clear();

    while(iterator->advance()) {
        iterator->getCurrent(tmpRecord);
        splitMe.append(tmpRecord);
    }

    pageTwo.clear();

	return recordPtr;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe) {
    MyDB_PageReaderWriter page = (*this)[whichPage];
    if(page.getType() == RegularPage) {
        if(page.append(appendMe)) {
            return nullptr;
        } else {
            return split(page, appendMe);
        }
    } else {
        MyDB_RecordIteratorAltPtr iteratorPtr = page.getIteratorAlt();
        MyDB_INRecordPtr recordPtr = getINRecord();

        while(iteratorPtr->advance()) {
            iteratorPtr->getCurrent(recordPtr);
            auto appendMeSmaller = buildComparator(appendMe, recordPtr);
            if(appendMeSmaller) {
                auto resRecPtr = append(recordPtr->getPtr(), appendMe);
                if(resRecPtr) {
                    if(page.append(resRecPtr)) {
                        MyDB_INRecordPtr tmp = getINRecord();
                        auto comp = buildComparator(resRecPtr, tmp);
                        page.sortInPlace(comp, resRecPtr, tmp);
                        return nullptr;
                    } else {
                        return split(page, resRecPtr);
                    }
                } else {
                    return nullptr;
                }
            }
        }
    }
    return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN re   cord
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
