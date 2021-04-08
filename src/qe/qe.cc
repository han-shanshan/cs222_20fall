#include "src/include/qe.h"
#include<iostream>
namespace PeterDB {
//    Filter::Filter(Iterator *input, const Condition &condition) {
//    }

    Filter::~Filter() {

    }
    bool getNullBit(char *nullIndicator, unsigned i) {
        return nullIndicator[i / CHAR_BIT] & ((unsigned) 1 << ((unsigned) 7 - (i % CHAR_BIT)));
    }
    RC Filter::getNextTuple(void *data) {
        char valueToFilter[PAGE_SIZE];
        while (this->iterator->getNextTuple(data) == 0) {
            memset(&valueToFilter, 0, PAGE_SIZE);
//            RelationManager::instance().printTuple(attrs, data, std::cout);
            getDataFieldVal(data, attrs, filterAttrPos, condition.rhsValue.type, &valueToFilter);
            if (getIsValueSatisfied(&valueToFilter)) {return 0; }
        }
        return QE_EOF;
    }


    bool Filter::getIsValueSatisfied(void *valueToFilter) const {
        bool isValueSatisfied = false;
        if (condition.op == NO_OP) { isValueSatisfied = true; }
        else {
            int compRes = ix.compareValue((char*)valueToFilter + 1, (char*)condition.rhsValue.data, condition.rhsValue.type);
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
        return 0;
    }


    RC Iterator::getDataFieldVal(const void *data, vector<Attribute> attrs, int attrPos, AttrType type, void * output) {
        int offset= ceil(attrs.size() / 8.0);
        char nullIndicator[offset];
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
//                float a = 0; //(*(char*)output + 1);
//                memcpy(&a, (char*)data + offset, 4);
//                cout<<"a="<<a<<endl;
            } else {
                memcpy((char*)output + 1, (char *) data + offset, sizeof(int) + *(int *) ((char *) data + offset));
            }
            free(nullsIndicator);
        }

        return 0;
    }


    RC Iterator::getTupleLength(const void *data, vector<Attribute> attrs) const {
        int nullIndicatorBitLen = ceil(attrs.size() / 8.0);
        int offset= nullIndicatorBitLen;
        char nullIndicator[offset];
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

        return offset - nullIndicatorBitLen;
    }





//    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
//
//    }

    Project::~Project() {

    }

    Project::Project(Iterator *input, const vector<string> &attrNames) : iterator(input) {
//    selectedAttributes.clear();
//    attributes.clear();
//        std::vector<Attribute> attrs;
        iterator->getAttributes(attributes);
        for (int j = 0; j < attrNames.size(); j++){
            for (int i = 0; i < attributes.size(); i++) {
                if (attributes[i].name.compare(attrNames[j]) == 0) {
                    selectedAttributes.push_back(attributes[i]);
                }
            }
        }
    }


    RC Project::getNextTuple(void *data) {
        int res = 0;
        char roughData[PAGE_SIZE];
        res = iterator->getNextTuple(roughData);

        if (res != 0) {return res; }
        unsigned attrOffset = ceil(this->attributes.size() / 8.0);
        unsigned selectedOffset = ceil(this->selectedAttributes.size() / 8.0);
        char nullIndicator[attrOffset];
        char selectedNullIndicator[selectedOffset];
        memset(selectedNullIndicator, 0, selectedOffset);
        memcpy(&nullIndicator, roughData, attrOffset);

        int i = 0, j = 0;
        int fieldLen = -1;
        for (; j < selectedAttributes.size(); j++) {
            attrOffset = ceil(this->attributes.size() / 8.0);
            for(i = 0; i < attributes.size(); i++) {
                if (!getNullBit(nullIndicator, i)) {
                    if ((attributes[i].type == TypeInt) || attributes[i].type == TypeReal) {
                        fieldLen = sizeof(int);
                    } else { //  TypeVarChar
                        fieldLen = *(int *) ((char *) roughData + attrOffset) + sizeof(int);
                    }
                } else { fieldLen = 0; }

                if (strcmp(attributes[i].name.c_str(), selectedAttributes[j].name.c_str()) == 0) {
                    if (fieldLen > 0) { //not null
                        memcpy((char *) data + selectedOffset, (char *) roughData + attrOffset, fieldLen);
                    } else {
                        selectedNullIndicator[j / 8] = 1;
                    }
                    selectedOffset += fieldLen;
                }
                attrOffset += fieldLen;
            }
        }
        memcpy(data, selectedNullIndicator, ceil(this->selectedAttributes.size() / 8.0));
        return res;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->selectedAttributes;
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        leftIn->getAttributes(leftAttrs);
        rightIn->getAttributes(rightAttrs);
        this->blockBuffer_numPages = numPages - 2;
        this->leftIter = leftIn;
        this->rightIter = rightIn;
        tupleLen_left = leftAttrs.size() * INT_FIELD_LEN + ceil(leftAttrs.size() / 8.0);
        tupleLen_right = rightAttrs.size() * INT_FIELD_LEN + ceil(rightAttrs.size() / 8.0);
        tupleLen_res = (rightAttrs.size() + leftAttrs.size()) * INT_FIELD_LEN + ceil((rightAttrs.size() + leftAttrs.size())/8.0);
        for (int i = 0; i < leftAttrs.size(); i++) {
            attrs.push_back(leftAttrs[i]);
            if(leftAttrs[i].type == TypeVarChar) {
                tupleLen_left += leftAttrs[i].length;
                tupleLen_res += leftAttrs[i].length;
            }
            if (leftAttrs[i].name.compare(condition.lhsAttr) == 0) {
                leftAttrPos = i;
            }
        }
        for (int i = 0; i < rightAttrs.size(); i++) {
            attrs.push_back(rightAttrs[i]);
            if(rightAttrs[i].type == TypeVarChar) {
                tupleLen_right += rightAttrs[i].length;
                tupleLen_res += rightAttrs[i].length;
            }
            if (rightAttrs[i].name.compare(condition.rhsAttr) == 0) {
                rightAttrPos = i;
            }
        }
        this->maxTupleNumInBlockBuffer = (PAGE_SIZE / tupleLen_left) * (this->blockBuffer_numPages);
        this->maxTupleNumInSingleBuffer = PAGE_SIZE / tupleLen_right;
        this->maxTupleNumPerResultPage = PAGE_SIZE / this->tupleLen_res;
        this->isNew = true;
    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
//        int res = -1;
        if(this->isNew) {
            // get new buffers: left buffers & a single right buffer (page 0) & result buffer
//            this->blockBuffer = malloc(PAGE_SIZE * this->blockBuffer_numPages);
            if(getLeftBlockBuffer() == 0){return -1; }
            if(getRightSingleBuffer(false) == 0){return -1;}
            if(getResultPage() == 0) {return -1;}
            blockBufferOffset = 0;
            singleBufferOffset = 0;
            resultBufferOffset = 0;
            blockCounter = 0;
            singleCounter = 0;
            resultCounter = 0;
            this->isNew = false;
        }
        if(this->resultCounter < tupleNum_ResultPage) {
            outputATupleInResultPage(data);
            return 0;
        }

        if(getRightSingleBuffer(false) == 0){
            if(getLeftBlockBuffer() > 0) {
                getRightSingleBuffer(true);
            }else {isLeftTableEnd = true;}
        }
        if(isLeftTableEnd) {return -1;}
        this->blockBufferOffset = 0;
        this->blockCounter = 0;
        getResultPage();
        blockBufferOffset = 0;
        singleBufferOffset = 0;
        resultBufferOffset = 0;
        blockCounter = 0;
        singleCounter = 0;
        resultCounter = 0;
        outputATupleInResultPage(data);
        return 0;
    }

    void BNLJoin::outputATupleInResultPage(void *data) {
        memcpy(data, resultBuffer + resultBufferOffset, tupleLen_res);
        resultBufferOffset += tupleLen_res;
        resultCounter++;
    }

    RC BNLJoin::getResultPage() {
        isResultPageFull = false;
        tupleNum_ResultPage = maxTupleNumPerResultPage;
        int lNullIndicatorLen = ceil(leftAttrs.size() / 8.0), rNullIndicatorLen = ceil(rightAttrs.size() / 8.0);
        int nullIndicatorBitLen = ceil((leftAttrs.size() + rightAttrs.size()) / 8.0);
        int ret = 0;
        while(1){
            ret = insertJoinedTuplesToRsBuffer(lNullIndicatorLen, rNullIndicatorLen, nullIndicatorBitLen);
            if(isResultPageFull){break;}
            if(getRightSingleBuffer(false) == 0){
                if(getLeftBlockBuffer() > 0) {
                    getRightSingleBuffer(true);
                    getResultPage();
                }else {isLeftTableEnd = true;}
            }else{
                this->blockBufferOffset = 0;
                this->blockCounter = 0;
                getResultPage();
            }
            if(isLeftTableEnd) {tupleNum_ResultPage = ret; break; }
        }
        return tupleNum_ResultPage;
    }

    RC BNLJoin::insertJoinedTuplesToRsBuffer(int lNullIndicatorLen, int rNullIndicatorLen, int nullIndicatorBitLen) {
        char lTuple[PAGE_SIZE];
        char lJoinVal[PAGE_SIZE], rJoinVal[PAGE_SIZE];
        char tuple[PAGE_SIZE];
        bool isJoinFinished = true;
        if(singleCounter > 0 && blockCounter < tupleNumInBlockBuffer) {
            isJoinFinished = false;
        }

        while(singleCounter < tupleNumInSingleBuffer) {
            singleCounter++;
            if(isJoinFinished) {
                memcpy(rTuple, singleBuffer + this->singleBufferOffset, tupleLen_right);
//                cout<<"right: ";
//                RelationManager::instance().printTuple(this->rightAttrs, rTuple, std::cout);
                singleBufferOffset += tupleLen_right;
            }else {isJoinFinished = true; }
            getDataFieldVal(rTuple, rightAttrs, rightAttrPos, rightAttrs[rightAttrPos].type, rJoinVal);
//            cout<<"rJoinVal="<<*(int*)(char*)(rJoinVal + 1)<<endl;
            while(this->blockCounter < tupleNumInBlockBuffer) {
                blockCounter++;
                memcpy(lTuple, (char*) blockBuffer + this->blockBufferOffset, tupleLen_left);
//                cout<<"left: ";
//                RelationManager::instance().printTuple(this->leftAttrs, lTuple, std::cout);
                blockBufferOffset += tupleLen_left;
                getDataFieldVal(lTuple, leftAttrs, leftAttrPos, leftAttrs[leftAttrPos].type, lJoinVal);

                if(ix.compareValue(lJoinVal + 1, rJoinVal + 1, leftAttrs[leftAttrPos].type) == 0) {
//                    cout<<"     lJoinVal="<<*(int*)(char*)(lJoinVal + 1)<<endl;
                    getJoinedTuple(lTuple, rTuple, lNullIndicatorLen, rNullIndicatorLen, nullIndicatorBitLen, tuple);
//                    RelationManager::instance().printTuple(this->attrs, tuple, std::cout);
                    memcpy(resultBuffer + resultBufferOffset, tuple, tupleLen_res);
                    resultBufferOffset += tupleLen_res;
                    if(resultBufferOffset == tupleLen_res * maxTupleNumPerResultPage) {
                        isResultPageFull = true;
                        break;
                    }
                }
            }
            blockBufferOffset = 0;
            blockCounter = 0;
            if(isResultPageFull) {break;}
        }
        if(isResultPageFull) {return this->maxTupleNumPerResultPage;}
        return resultBufferOffset/tupleLen_res;
    }

    void BNLJoin::getJoinedTuple(void *lTuple, void *rTuple, int lNullIndicatorLen, int rNullIndicatorLen,
                                 int nullIndicatorBitLen, void *tuple) const {
        unsigned char nullIndicator[nullIndicatorBitLen];
        int lLen = getTupleLength(lTuple, leftAttrs);
        int rLen = getTupleLength(rTuple, rightAttrs);
        memset(nullIndicator, 0, nullIndicatorBitLen);
        memcpy(&nullIndicator, lTuple, lNullIndicatorLen);

        for(int i = 0; i < rightAttrs.size(); i++) {
            if(getNullBit((char*)rTuple, i)) {
                nullIndicator[(leftAttrs.size() + i) / 8] = 1;
            }
        }
        memcpy(tuple, nullIndicator, nullIndicatorBitLen);
        memcpy((char*)tuple + nullIndicatorBitLen, (char*)lTuple + lNullIndicatorLen, lLen);
        memcpy((char*)tuple + nullIndicatorBitLen + lLen, (char*)rTuple + rNullIndicatorLen, rLen);
    }

    RC BNLJoin::getRightSingleBuffer(bool isFromBegin) {
        int rightOffset = 0;
        char tempTuple[PAGE_SIZE];
        int counter = 0;
        tupleNumInSingleBuffer = maxTupleNumInSingleBuffer;
        if(isFromBegin){
            rightIter->setIterator();
//            rightIter.Iter.rbfm_scanner.lastRID.pageNum = 0;

//            rightIter.
        }
        int bitNum = ceil(rightAttrs.size() / 8.0);
        char nullBit[bitNum];
        memset(nullBit, 0, bitNum);


        while(counter < maxTupleNumInSingleBuffer) {
            if(rightIter->getNextTuple(tempTuple) != 0) {
                tupleNumInSingleBuffer = counter;
                isRightTableEnd = true;
                break;
            }
//            cout<<"getRightBlockBuffer: ";
//            RecordBasedFileManager::instance().printRecord(rightAttrs, tempTuple, std::cout);
//            RelationManager::instance().printTuple(this->rightAttrs, tempTuple, std::cout);
            memcpy((char*) singleBuffer + rightOffset, tempTuple, tupleLen_right);
            rightOffset += tupleLen_right;
            counter++;
        }
        return tupleNumInSingleBuffer;
    }

    RC BNLJoin::getLeftBlockBuffer() {
        free(this->blockBuffer);
        this->blockBuffer = malloc(PAGE_SIZE * this->blockBuffer_numPages);
        int leftReadCounter = 0, leftOffset = 0;
        tupleNumInBlockBuffer = maxTupleNumInBlockBuffer;
        char tempTuple[PAGE_SIZE];
        while(leftReadCounter < maxTupleNumInBlockBuffer) {
            if(leftIter->getNextTuple(tempTuple) != 0) {

                tupleNumInBlockBuffer = leftReadCounter;
                this->isLeftTableEnd = true;
                break;
            }
//            cout<<"getLeftBlockBuffer: ";
//            RelationManager::instance().printTuple(this->leftAttrs, tempTuple, std::cout);
            memcpy((char*)blockBuffer + leftOffset, tempTuple, tupleLen_left);
            leftOffset += tupleLen_left;
            leftReadCounter++;
        }
        return tupleNumInBlockBuffer;
    }

//    bool BNLJoin::isBufferProcessingFinished() const {
//        return blockCounter == this->maxTupleNumInBlockBuffer
//               && singleBufferTupleReadCounter == maxTupleNumInSingleBuffer
//               && resultReadCounter == maxTupleNumPerResultPage;
//    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->attrs;
        return 0;
    }

//    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
//
//    }

    INLJoin::~INLJoin() {

    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        leftIn->getAttributes(leftAttrs);
        rightIn->getAttributes(rightAttrs);
        this->isRightEnd = true;
        this->isEnd = false;
        this->leftIter = leftIn;
        this->rightIter = rightIn;

        for (int i = 0; i < leftAttrs.size(); i++) {
            attrs.push_back(leftAttrs[i]);
            if (leftAttrs[i].name.compare(condition.lhsAttr) == 0) {
                leftAttrPos = i;
            }
        }

        for (int i = 0; i < rightAttrs.size(); i++) {
            attrs.push_back(rightAttrs[i]);
            if (rightAttrs[i].name.compare(condition.rhsAttr) == 0) {
                rightAttrPos = i;
            }
        }
    }

    RC INLJoin::getNextTuple(void *data) {
        int res = -1;
        if(isRightEnd) {
            res = leftIter->getNextTuple(lTuple);
//            RelationManager::instance().printTuple(leftAttrs, lTuple, std::cout);
            if (res != 0) {
                return -1;
            }
            getDataFieldVal(lTuple, leftAttrs, leftAttrPos,
                            leftAttrs[leftAttrPos].type, leftValue);
            isRightEnd = false;
            rightIter->setIterator((char*)leftValue + 1, (char*)leftValue + 1, true, true);
            float a = *(float*)((char*)leftValue + 1);
            cout<<a<<endl;
        }
        if (rightIter->getNextTuple(rTuple) != 0) {
            isRightEnd = true;
            if (getNextTuple(data) != 0) {
                return -1;
            } else return 0;
        }

        int lLen = getTupleLength(lTuple, leftAttrs);
        int rLen = getTupleLength(rTuple, rightAttrs);
        int lNullIndicatorLen = ceil(leftAttrs.size() / 8.0);
        int rNullIndicatorLen = ceil(rightAttrs.size() / 8.0);

        int nullIndicatorBitLen = ceil((leftAttrs.size() + rightAttrs.size()) / 8.0);
        unsigned char nullIndicator[nullIndicatorBitLen];
        memset(nullIndicator, 0, nullIndicatorBitLen);
        memcpy(&nullIndicator, lTuple, lNullIndicatorLen);

        for(int i = 0; i < rightAttrs.size(); i++) {
            if(getNullBit((char*)rTuple, i)) {
                nullIndicator[(leftAttrs.size() + i)/8] = 1;
            }
        }
        memcpy(data, nullIndicator, nullIndicatorBitLen);
        memcpy((char*)data + nullIndicatorBitLen, (char*)lTuple + lNullIndicatorLen, lLen);
        memcpy((char*)data + nullIndicatorBitLen + lLen, (char*)rTuple + rNullIndicatorLen, rLen);

        return 0;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
//        attrs.clear();
        attrs = this->attrs;
        return 0;
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

//    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
//
//    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        this->type = aggAttr.type;
        this->op = op;
        this->aggrAttribute = aggAttr;
        this->iterator = input;
        this->isNew = true;
        this->groupAttr = groupAttr;
        this->isGroup = true;

        input->getAttributes(attributes);
        for(int i = 0; i < attributes.size(); i++) {
            if(aggAttr.name.compare(attributes[i].name) == 0) {
                this->attrPos = i;
            }
            if(groupAttr.name.compare(attributes[i].name) == 0) {
                this->groupAttrPos = i;
            }
        }
    }

    Aggregate::~Aggregate() {

    }


    //  MIN = 0, MAX, COUNT, SUM, AVG   AggregateOp;
    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op){
        this->type = aggAttr.type;
        this->op = op;
        this->aggrAttribute = aggAttr;
        this->iterator = input;
        this->isNew = true;

        input->getAttributes(attributes);
        for(int i = 0; i < attributes.size(); i++) {
            if(aggAttr.name.compare(attributes[i].name) == 0) {
                this->attrPos = i;
            }
        }
    }

    RC Aggregate::getNextTuple(void *data) {
        char tempData[PAGE_SIZE];

        if(this->iterator->getNextTuple(tempData) != 0) {return -1;}

//        if(isGroup) {
//            char groupAttrVal[PAGE_SIZE];
//            getDataFieldVal(tempData, this->attributes, groupAttrPos, this->groupAttr.type, groupAttrVal);
            //todo: put it into vector; set new iterator;
//            RelationManager::instance().scan((TableScan)this->iterator.getFileName(), "", NO_OP, NULL, attrNames, iter);



//        }


        while(true) {
//            RelationManager::instance().printTuple(this->attributes, tempData, std::cout);
            if(this->op == MAX || this->op == MIN) {
                extremeValue(tempData, this->op);
            }else if(this->op == AVG) {
                Avg(tempData);
            }
            if(this->iterator->getNextTuple(tempData) != 0) {break;}
        }

        if(this->op == MAX || this->op == MIN) {
            memcpy(data, this->aggrVal, PAGE_SIZE);
        }else{
            float avg = (float)sum / counter;
            char nullBit[1];
            memset(nullBit, 0, 1);
            memcpy(data, nullBit, 1);
            memcpy((char*)data + 1, &avg, INT_FIELD_LEN);
        }

        return 0;
    }

    RC Aggregate::Avg(void *data) {
        char output[PAGE_SIZE];
        getDataFieldVal(data, this->attributes, attrPos, this->type, output);
        if(isNew) {
            isNew = false;
            sum = *(int*)((char*)(output + 1));
            this->counter = 1;
            return 0;
        }else {
            int curNum = *(int*)((char*)(output + 1));
            sum += curNum;
            counter ++;
        }
        return 0;
    }

    RC Aggregate::extremeValue(void *data, AggregateOp op) {
        char output[PAGE_SIZE];
        getDataFieldVal(data, this->attributes, attrPos, this->type, output);
        if(isNew) {
            isNew = false;
            setAggregationVal(output);
            return 0;
        }
        if(op == MAX){
            if(ix.compareValue(this->aggrVal + 1, output + 1, this->type) < 0) {
                setAggregationVal(output);
            }
        }else {
            if(ix.compareValue(this->aggrVal + 1, output + 1, this->type) > 0) {
                setAggregationVal(output);
            }
        }

        return 0;
    }

    void Aggregate::setAggregationVal(void *aggreData) const {
        int len = 0;
        if(type == TypeVarChar) {
            memcpy(&len, (char*)aggreData + 1, INT_FIELD_LEN);
        }
        len = len + 1 + INT_FIELD_LEN;
        memcpy((char*)aggrVal, aggreData, len);
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
//        attrs = this->attributes;
        attrs.clear();
        cout<<attributes.size()<<endl;
        Attribute a = attributes[this->attrPos];
        switch (this->op) {
            case MAX:
                a.name = "MAX(";
                break;
            case MIN:
                a.name = "MIN(";
                break;
            case AVG:
                a.name = "AVG(";
                a.type = TypeReal;
                break;
        }

        a.name = a.name + attributes[this->attrPos].name + ")";
        attrs.push_back(a);
        return 0;
    }
} // namespace PeterDB