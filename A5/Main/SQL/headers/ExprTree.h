
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include <string>
#include <vector>
#include "MyDB_Catalog.h"

// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr <ExprTree> ExprTreePtr;

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree {

public:
	virtual string toString () = 0;
	virtual ~ExprTree () {}
    virtual string getExpType () = 0;
    virtual bool check (MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) = 0;
};

class BoolLiteral : public ExprTree {

private:
	bool myVal;
public:
	
	BoolLiteral (bool fromMe) {
		myVal = fromMe;
	}

	string toString () {
		if (myVal) {
			return "bool[true]";
		} else {
			return "bool[false]";
		}
	}

    string getExpType () {
        return "BOOLEAN";
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        return true;
    }
};

class DoubleLiteral : public ExprTree {

private:
	double myVal;
public:

	DoubleLiteral (double fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "double[" + to_string (myVal) + "]";
	}

    string getExpType () {
        return "NUMBER";
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) {
        return true;
    }

	~DoubleLiteral () {}
};

// this implement class ExprTree
class IntLiteral : public ExprTree {

private:
	int myVal;
public:

	IntLiteral (int fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "int[" + to_string (myVal) + "]";
	}

    string getExpType () {
        return "NUMBER";
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        return true;
    }

	~IntLiteral () {}
};

class StringLiteral : public ExprTree {

private:
	string myVal;
public:

	StringLiteral (char *fromMe) {
		fromMe[strlen (fromMe) - 1] = 0;
		myVal = string (fromMe + 1);
	}

	string toString () {
		return "string[" + myVal + "]";
	}

    string getExpType () {
        return "STRING";
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        return true;
    }

	~StringLiteral () {}
};

class Identifier : public ExprTree {

private:
	string tableName;
	string attName;
    string expType = "IDENTIFIER";
public:

	Identifier (char *tableNameIn, char *attNameIn) {
		tableName = string (tableNameIn);
		attName = string (attNameIn);
	}

	string toString () {
		return "[" + tableName + "_" + attName + "]";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        string tmpTable, tmpAttr;
        for(pair<string, string> table: tables) {
            if (table.second == tableName) {
                tmpTable = table.first;
            }
        }
        if (tmpTable.empty()) {
            cout << "Error: Table " + tableName + " not found!" <<endl;
            return false;
        }

        bool correct = catalog->getString(tmpTable + "." + attName + ".type", tmpAttr);
        if (!correct) {
            cout << "Error: Attribute " + attName + " does not exist in table " + tmpTable << endl;
            return false;
        }

        if (tmpAttr == "int" || tmpAttr == "double") { // may need change?
            expType = "NUMBER";
        } else if (tmpAttr == "string") {
            expType = "STRING";
        } else {
            expType = "BOOLEAN";
        }
        return true;
    }

	~Identifier () {}
};

class MinusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
    string expType = "NUMBER";
	
public:

	MinusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "- (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!lhs->check(catalog, tables) || !rhs->check(catalog, tables)) {
            return false;
        }

        string lhsType = lhs->getExpType(), rhsType = rhs->getExpType();

        if (lhsType != "NUMBER" || rhsType != "NUMBER") {
            cout << "Error: In minus operation, one of the variables is not number!" << endl;
            return false;
        }
        return true;
    }



	~MinusOp () {}
};

class PlusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
    string expType = "STRING";
	
public:

	PlusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "+ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!lhs->check(catalog, tables) || !rhs->check(catalog, tables)) {
            return false;
        }

        string lhsType = lhs->getExpType(), rhsType = rhs->getExpType();

        if (lhsType != rhsType) {
            cout << "Error: In plus operation, the type of two variables are not the same!" << endl;
            return false;
        }

        if (lhsType == "STRING") {
            return true;
        } else if (lhsType == "NUMBER") {
            expType = "NUMBER";
            return true;
        } else {
            cout << "Error: In plus operation, the types of variable allowed are only string or number!" << endl;
            return false;
        }
    }

	~PlusOp () {}
};

class TimesOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
    string expType = "NUMBER";
	
public:

	TimesOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "* (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!lhs->check(catalog, tables) || !rhs->check(catalog, tables)) {
            return false;
        }

        string lhsType = lhs->getExpType(), rhsType = rhs->getExpType();

        if (lhsType != "NUMBER" || rhsType != "NUMBER") {
            cout << "Error: In times operation, one of the variables is not number!" << endl;
            return false;
        }
        return true;
    }

	~TimesOp () {}
};

class DivideOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
    string expType = "NUMBER";
	
public:

	DivideOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "/ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!lhs->check(catalog, tables) || !rhs->check(catalog, tables)) {
            return false;
        }

        string lhsType = lhs->getExpType(), rhsType = rhs->getExpType();

        if (lhsType != "NUMBER" || rhsType != "NUMBER") {
            cout << "Error: In divide operation, one of the variables is not number!" << endl;
            return false;
        }
        return true;
    }

	~DivideOp () {}
};

class GtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
    string expType = "BOOLEAN";
	
public:

	GtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "> (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!lhs->check(catalog, tables) || !rhs->check(catalog, tables)) {
            return false;
        }

        string lhsType = lhs->getExpType(), rhsType = rhs->getExpType();

        if (lhsType != rhsType) {
            cout << "Error: In Greater Than operation, the two variables' type are not the same" << endl;
            return false;
        }
        return true;
    }

	~GtOp () {}
};

class LtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
    string expType = "BOOLEAN";
	
public:

	LtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "< (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!lhs->check(catalog, tables) || !rhs->check(catalog, tables)) {
            return false;
        }

        string lhsType = lhs->getExpType(), rhsType = rhs->getExpType();

        if (lhsType != rhsType) {
            cout << "Error: In Less Than operation, the two variables' type are not the same" << endl;
            return false;
        }
        return true;
    }

	~LtOp () {}
};

class NeqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
    string expType = "BOOLEAN";
	
public:

	NeqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "!= (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!lhs->check(catalog, tables) || !rhs->check(catalog, tables)) {
            return false;
        }

        string lhsType = lhs->getExpType(), rhsType = rhs->getExpType();

        if (lhsType != rhsType) {
            cout << "Error: In Not Equal operation, the two variables' type are not the same" << endl;
            return false;
        }
        return true;
    }

	~NeqOp () {}
};

class OrOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
    string expType = "BOOLEAN";
	
public:

	OrOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "|| (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!lhs->check(catalog, tables) || !rhs->check(catalog, tables)) {
            return false;
        }

        string lhsType = lhs->getExpType(), rhsType = rhs->getExpType();

        if (rhsType != "BOOLEAN" || lhsType != "BOOLEAN") {
            cout << "Error: In Or Operation, at least one of them are not boolean type" << endl;
            return false;
        }
        return true;
    }

	~OrOp () {}
};

class EqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
    string expType = "BOOLEAN";
	
public:

	EqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "== (" + lhs->toString () + ", " + rhs->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!lhs->check(catalog, tables) || !rhs->check(catalog, tables)) {
            return false;
        }

        string lhsType = lhs->getExpType(), rhsType = rhs->getExpType();

        if (lhsType != rhsType) {
            cout << "Error: In Equal operation, the two variables' type are not the same" << endl;
            return false;
        }
        return true;
    }

	~EqOp () {}
};

class NotOp : public ExprTree {

private:

	ExprTreePtr child;
    string expType = "BOOLEAN";
	
public:

	NotOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "!(" + child->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!child->check(catalog, tables)) {
            return false;
        }

        string childType = child->getExpType();

        if (childType != "BOOLEAN") {
            cout << "Error: In Not operation, the variable type is not boolean" << endl;
            return false;
        }
        return true;
    }

	~NotOp () {}
};

class SumOp : public ExprTree {

private:

	ExprTreePtr child;
    string expType = "NUMBER";
	
public:

	SumOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "sum(" + child->toString () + ")";
	}

    string getExpType () {
        return "NUMBER";
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!child->check(catalog, tables)) {
            return false;
        }

        string childType = child->getExpType();

        if (childType != "NUMBER") {
            cout << "Error: In Sum operation, the variable type is not number" << endl;
            return false;
        }
        return true;
    }

	~SumOp () {}
};

class AvgOp : public ExprTree {

private:

	ExprTreePtr child;
    string expType = "NUMBER";
	
public:

	AvgOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "avg(" + child->toString () + ")";
	}

    string getExpType () {
        return expType;
    }

    bool check(MyDB_CatalogPtr catalog, vector<pair<string, string>> tables) override {
        if (!child->check(catalog, tables)) {
            return false;
        }

        string childType = child->getExpType();

        if (childType != "NUMBER") {
            cout << "Error: In Avg operation, the variable type is not number" << endl;
            return false;
        }
        return true;
    }

	~AvgOp () {}
};

#endif
