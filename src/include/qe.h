#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>

#include "rm.h"
#include "ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;

        RC getDataFieldVal(const void *data, vector<Attribute> attrs, int attrPos, AttrType type, void *output);

        RC getTupleLength(const void *data, vector<Attribute> attrs) const;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        string getTableName() {
            return this->tableName;
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
//            iter.ixFileHandle
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;


            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
        // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        IndexManager &ix = IndexManager::instance();

        ~Filter() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

    private:
        Condition condition;
        unsigned filterAttrPos;
        std::vector<Attribute> attrs;
        Iterator *iterator = nullptr;

        bool getIsValueSatisfied(void *valueToFilter) const;

//    void getValueToFilter(void *data, void *valueToFilter) const;
    };

    class Project : public Iterator {
        // Projection operator
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        Condition condition;
//    unsigned filterAttrPos;
        std::vector<Attribute> selectedAttributes;
        std::vector<Attribute> attributes;
        Iterator *iterator = nullptr;
    };

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;
        IndexManager &ix = IndexManager::instance();
        std::vector<Attribute> leftAttrs;
        std::vector<Attribute> rightAttrs;
        int blockBuffer_numPages;
        unsigned leftAttrPos;
        unsigned rightAttrPos;
        int maxTupleNumInSingleBuffer = 0;
        int maxTupleNumInBlockBuffer = 0;
        int maxTupleNumPerResultPage = 0;
        int tupleNumInSingleBuffer = 0;
        int tupleNumInBlockBuffer = 0;
        int tupleNum_ResultPage = 0;
        char rTuple[PAGE_SIZE];

        Iterator *leftIter;
        TableScan *rightIter;
        void *blockBuffer = malloc(PAGE_SIZE);
        char singleBuffer[PAGE_SIZE];
        char resultBuffer[PAGE_SIZE];
        bool isNew;
        bool isRightTableEnd = false;
        int blockBufferOffset = 0;
        int singleBufferOffset = 0;
        int resultBufferOffset = 0;
        int blockCounter = 0;
        int singleCounter = 0;
        int resultCounter = 0;
        int tupleLen_left = 0;
        int tupleLen_right = 0;
        int tupleLen_res = 0;
        bool isResultPageFull = false;
        bool isLeftTableEnd = false;

        std::vector<Attribute> attrs;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

//        bool isBufferProcessingFinished() const;

        RC getLeftBlockBuffer();

        RC getRightSingleBuffer(bool isFromBegin);

        void getJoinedTuple(void *lTuple, void *rTuple, int lNullIndicatorLen, int rNullIndicatorLen,
                            int nullIndicatorBitLen,
                            void *tuple) const;

        RC getResultPage();

        RC insertJoinedTuplesToRsBuffer(int lNullIndicatorLen, int rNullIndicatorLen, int nullIndicatorBitLen);

        void outputATupleInResultPage(void *data);
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        bool isRightEnd = true;
        Iterator *leftIter;
        IndexScan *rightIter;

        char leftValue[PAGE_SIZE];

        char lTuple[PAGE_SIZE];
        char rTuple[PAGE_SIZE];

//    CompOp op;
//    AttrType type;

        unsigned leftAttrPos;
        unsigned rightAttrPos;

        std::vector<Attribute> attrs;
        std::vector<Attribute> leftAttrs;
        std::vector<Attribute> rightAttrs;

        RC isEnd;
        bool leftHalf; // for NE_OP;
//    AttrType type;

        void setCondition(CompOp op, void **lowKey, void **highKey, bool &lowKeyInclusive, bool &highKeyInclusive);

    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate();

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;

    private:
        Iterator *iterator;
        AggregateOp op;
        Attribute aggrAttribute;
        Attribute groupAttr;
        AttrType type;
        std::vector<Attribute> attributes;
        int attrPos;
        int groupAttrPos;
        bool isNew;
        char aggrVal[PAGE_SIZE];
        int counter = 0;
        int sum = 0;
        bool isGroup = false;
        std::vector<string> groups;
//        char aggrTuple[PAGE_SIZE];

        RC Sum(void *data);

        RC Avg(void *data);
        RC Count(void *data);
//    RC Max(void *data);
//    RC Min(void *data);

        void *setVal(void *minVal, const void *fieldDataVal) const;

        RC extremeValue(void *data, AggregateOp op);
        IndexManager &ix = IndexManager::instance();

        void setAggregationVal(void *data) const;
    };
} // namespace PeterDB

#endif // _qe_h_
