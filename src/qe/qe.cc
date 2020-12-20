#include "src/include/qe.h"

namespace PeterDB {
//    Filter::Filter(Iterator *input, const Condition &condition) {
//    }

    Filter::~Filter() {

    }
    bool getNullBit(unsigned char *nullIndicator, unsigned i) {
        return nullIndicator[i / CHAR_BIT] & ((unsigned) 1 << ((unsigned) 7 - (i % CHAR_BIT)));
    }
    RC Filter::getNextTuple(void *data) {
        char valueToFilter[PAGE_SIZE];
//    cout << "in get next" << endl;
//    cout << this->iterator->getNextTuple(data) << endl;
        while (this->iterator->getNextTuple(data) == 0) {
//        cout << "in while next" << endl;
            memset(&valueToFilter, 0, PAGE_SIZE);
            getDataFieldVal(data, attrs, filterAttrPos, condition.rhsValue.type, &valueToFilter);

            if (getIsValueSatisfied(&valueToFilter)) {
//            cout << "get one match" << endl;
//            cout << *((int *) &valueToFilter) << endl;
                return 0;
            }
        }
        return QE_EOF;
    }


    bool Filter::getIsValueSatisfied(void *valueToFilter) const {
        bool isValueSatisfied = false;
        if (condition.op == NO_OP) { isValueSatisfied = true; }
        else {
            int compRes = ix.compareValue(valueToFilter, condition.rhsValue.data, condition.rhsValue.type);
            if ((condition.op == EQ_OP && compRes == 0)
                || (condition.op == LT_OP && compRes < 0)
                || (condition.op == GT_OP && compRes > 0)
                || (condition.op == LE_OP && compRes <= 0)
                || (condition.op == GE_OP && compRes >= 0)
                || (condition.op == NE_OP && compRes != 0)) {
                isValueSatisfied = true;
            }
        }
        return isValueSatisfied;
    }


    Filter::Filter(Iterator *input, const Condition &cond) : iterator(input), condition(cond) {

        // get the attributes from input Iterator
        getAttributes(attrs);

        for (int i = 0; i < attrs.size(); i++) {
            if (attrs[i].name.compare(this->condition.lhsAttr) == 0) {
                filterAttrPos = i;
                break;
            }
        }
    }



    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        this->iterator->getAttributes(attrs);
    }


    RC Iterator::getDataFieldVal(const void *data, vector<Attribute> attrs, int attrPos, AttrType type, void * output) {
        int offset= ceil(attrs.size() / 8.0);
        unsigned char nullIndicator[offset];
        memcpy(&nullIndicator, data, offset);
        for (int i = 0; i < attrPos; i++) {
            //1: null; 0: not null
            if (!getNullBit(nullIndicator, i)) {
                if ((attrs[i].type == TypeInt) || attrs[i].type == TypeReal) {
                    offset += sizeof(int);
                } else { //  TypeVarChar
                    offset += *(int *) ((char *) data + offset) + (int) sizeof(int);
                }
            }
        }

        if (!getNullBit(nullIndicator, attrPos)) {
            void * nullsIndicator = malloc(1);
            memset(nullsIndicator, 0, 1);//00000000
            memcpy(output, nullIndicator, 1);
            if (type == TypeInt || type == TypeReal) {
                memcpy((char*)output + 1, (char *) data + offset, sizeof(int));
            } else {
                memcpy((char*)output + 1, (char *) data + offset, sizeof(int) + *(int *) ((char *) data + offset));
            }
            free(nullsIndicator);
        }

        return 0;
    }


    RC Iterator::getTupleLength(const void *data, vector<Attribute> attrs) {
        int offset= ceil(attrs.size() / 8.0);
        unsigned char nullIndicator[offset];
        memcpy(&nullIndicator, data, offset);
        for (int i = 0; i < attrs.size(); i++) {
            //1: null; 0: not null
            if (!getNullBit(nullIndicator, i)) {
                if ((attrs[i].type == TypeInt) || attrs[i].type == TypeReal) {
                    offset += sizeof(int);
                } else { //  TypeVarChar
                    offset += *(int *) ((char *) data + offset) + (int) sizeof(int);
                }
            }
        }

        return offset;
    }



//    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
//
//    }

    Project::~Project() {

    }

    Project::Project(Iterator *input, const vector<string> &attrNames) : iterator(input) {
//    selectedAttributes.clear();
//    attributes.clear();

        iterator->getAttributes(attributes);

        int j = 0;
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes[i].name.compare(attrNames[j]) == 0) {
                selectedAttributes.push_back(attributes[i]);
                j++;
            }
        }

//    tuple = malloc(PAGE_SIZE);
    }


    RC Project::getNextTuple(void *data) {
        int res = 0;
        void *roughData = malloc(PAGE_SIZE);
        res = iterator->getNextTuple(roughData);
//    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
//    rbfm.printStr(20, (char*)roughData);

        if (res != 0) {return res; }
        unsigned attrOffset = ceil(this->attributes.size() / 8.0);

        unsigned selectedOffset = ceil(this->selectedAttributes.size() / 8.0);
//    cout<<"attr = "<<this->attributes.size() <<", selected = "<<this->selectedAttributes.size() <<endl;
        unsigned char nullIndicator[attrOffset];
        unsigned char selectedNullIndicator[selectedOffset];
        memset(selectedNullIndicator, 0, selectedOffset);
        memcpy(&nullIndicator, roughData, attrOffset);

//    project
        int i = 0, j = 0;
        int fieldLen = -1;
        for (; i < attributes.size() && j < selectedAttributes.size(); i++) {
            if (!getNullBit(nullIndicator, i)) {
                if ((attributes[i].type == TypeInt) || attributes[i].type == TypeReal) {
                    fieldLen = sizeof(int);
                } else { //  TypeVarChar
                    fieldLen = *(int *) ((char *) roughData + attrOffset) + sizeof(int);
                }

            }else {fieldLen = 0; }

            if (strcmp(attributes[i].name.c_str(), selectedAttributes[j].name.c_str()) == 0) {
                if(fieldLen > 0){ //not null
                    memcpy((char *)data + selectedOffset, (char *)roughData + attrOffset, fieldLen);
                }else {
                    selectedNullIndicator[j / 8] = 1;
                }
                j++;
                selectedOffset += fieldLen;
            }
            attrOffset += fieldLen;

        }
        memcpy(data, selectedNullIndicator, ceil(this->selectedAttributes.size() / 8.0));
        free(roughData);


//    unsigned attrOffset = ceil(this->attributes.size() / 8.0);
//    unsigned selectedOffset = ceil(this->selectedAttributes.size() / 8.0);
//    bool isFieldNull;
//    unsigned char nullIndicator[attrOffset];
//    unsigned char selectedNullIndicator[selectedOffset];
//    memcpy(&nullIndicator, input, attrOffset);
//    for (int i = 0; i < this->attributes.size(); i++) {
//        //1: null; 0: not null
//        if (!getNullBit(nullIndicator, i)) {
//            if ((attrs[i].type == TypeInt) || attrs[i].type == TypeReal) {
//                attrOffset += sizeof(int);
//            } else { //  TypeVarChar
//                attrOffset += *(unsigned *) ((char *) input + attrOffset) + (unsigned) sizeof(int);
//            }
//        }
//    }

//    isFieldNull = getNullBit(nullIndicator, filterAttrPos);
//    if (!isFieldNull) {
//        if (condition.rhsValue.type == TypeInt || condition.rhsValue.type == TypeReal) {
//            memcpy(valueToFilter, (char *) input + offset, sizeof(int));
//        } else {
//            memcpy(valueToFilter, (char *) input + offset, sizeof(int) + *(int *) ((char *) input + offset));
//        }
//    }

        return res;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->selectedAttributes;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {

    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

//    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
//
//    }

    INLJoin::~INLJoin() {

    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition)
            : lIterator(leftIn), rIterator(rightIn) {
        attrs.clear();
        leftAttrs.clear();
        rightAttrs.clear();

        leftIn->getAttributes(leftAttrs);
        rightIn->getAttributes(rightAttrs);

        for (int i = 0; i < leftAttrs.size(); i++) {
            attrs.push_back(leftAttrs[i]);
            /**
             * ToDo: 用。分割
             * leftAttrs[leftAttrPos].type
             */
            if (leftAttrs[i].name.compare(condition.lhsAttr) == 0) {
                leftAttrPos = i;
//            type = leftAttrs[i].type;
                leftValue = malloc(leftAttrs[i].length + sizeof(int));
            }
        }

        for (int i = 0; i < rightAttrs.size(); i++) {
//        /**
//         * Todo:
//         */
            attrs.push_back(rightAttrs[i]);
            if (leftAttrs[leftAttrPos].name.compare(rightAttrs[i].name) == 0) {
                rightAttrPos = i;
            }
        }
    }

    RC INLJoin::getNextTuple(void *data) {
        int res = -1;
        if(isRightEnd) {
            lTuple = malloc(PAGE_SIZE);
            res = lIterator->getNextTuple(lTuple);
            if (res != 0) {
                free(lTuple);
                return -1;
            }
            leftValue = malloc(PAGE_SIZE);
            getDataFieldVal(lTuple, leftAttrs, leftAttrPos,
                            leftAttrs[leftAttrPos].type, leftValue);
            isRightEnd = false;
            rIterator->setIterator(leftValue, leftValue, true, true);

//    isEnd = lItr->getNextTuple(leftTuple);
//    if (isEnd != QE_EOF) {
//        readField(leftTuple, leftValue, leftAttrs, leftAttrPos, type);
//
//        void *lowKey = NULL;
//        void *highKey = NULL;
//        bool lowKeyInclusive = false;
//        bool highKeyInclusive = false;
//
//        setCondition(op, &lowKey, &highKey, lowKeyInclusive, highKeyInclusive);
//        rightItr->setIterator(lowKey, highKey, lowKeyInclusive, highKeyInclusive);
//
        }
        rTuple = malloc(PAGE_SIZE);
        if (rIterator->getNextTuple(rTuple) != 0) {
//                rightFieldVal = malloc(PAGE_SIZE);
//                getDataFieldVal(rTuple, rightAttrs, rightAttrPos,
//                                rightAttrs[rightAttrPos].type, rightFieldVal);
//                if (compareValue(leftValue, rightFieldVal, rightAttrs[rightAttrPos].type) == 0) {
//                    free(rightFieldVal);
//                    break;
//                }
//                free(rightFieldVal);
            isRightEnd = true;
            free(leftValue);
            free(lTuple);
            free(rTuple);
            if (getNextTuple(data) != 0) {
                return -1;
            } else return 0;
        }

        /**
         * ToDo 处理null indicator
         */
        int lLen = getTupleLength(lTuple, leftAttrs);
        memcpy(data, lTuple, lLen);
        memcpy((char *)data + lLen, rTuple, getTupleLength(rTuple, rightAttrs));

        free(rTuple);
        return 0;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attrs;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

//    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
//
//    }

    Aggregate::~Aggregate() {

    }


//MIN = 0, MAX, COUNT, SUM, AVG   AggregateOp;
    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op){
        this->type = aggAttr.type;
        this->op = op;
        this->aggrAttribute = aggAttr;
        this->iterator = input;

//
//    short field_no = 0;
//    int max_tuple_size = 0;
//    isNextTuple = true;
//
        input->getAttributes(attributes);
        for(int i = 0; i < attributes.size(); i++) {
            if(aggAttr.name.compare(attributes[i].name) == 0) {
                this->attrPos = i;
            }
        }
//    for(int i = 0; i < tblAttributes.size(); i++) {
//
//        Attribute attribute = tblAttributes[i];
//        if (attribute.type == TypeVarChar) {
//            max_tuple_size += sizeof(int);
//        }
//
//        string attrName = tblAttributes[i].name;
//        if(attrName.compare(aggAttr.name) == 0) {
//            field_no = i;
//            max_tuple_size += tblAttributes[i].length;
//            break;
//        }
//        max_tuple_size += tblAttributes[i].length;
//    }
//
//    this->attrPos = field_no;
//    this->max_tuple_size = max_tuple_size;

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB