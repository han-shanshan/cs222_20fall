#include <iostream>
#include "src/include/rbfm.h"
//#include "cmath"
#include <math.h>
#include <vector>
using namespace std;
namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

//    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        if (pfm.createFile(fileName) != 0) { return -1; } //cout << "RecordBasedFileManager fails to create the file." << endl;
        return 0;
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        if (pfm.destroyFile(fileName) != 0) {return -1; } //fail to destroy the file.
        return 0;
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if (pfm.openFile(fileName.c_str(), fileHandle) != 0) {return -1; } //fail to open the file.
        return 0;
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        if (pfm.closeFile(fileHandle) != 0) {return -1; } //fail to close the file.
        return 0;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        //rid: page num, slot num;
        void *encodedData = malloc(PAGE_SIZE);
        int slotLength = encodeRecordData_returnSlotLength(recordDescriptor, data, encodedData);
        if (fileHandle.file == NULL) {return -1; } //error;
//    cout<<"slotlength = "<<slotLength<<endl;
        int res = insertEncodedRecord(fileHandle, rid, encodedData, slotLength);
        free(encodedData);
        if(res != 0 ){return -1;}
        return 0;
    }

    RC RecordBasedFileManager::insertEncodedRecord(FileHandle &fileHandle, RID &rid, const void *encodedData, int slotLength) {
        int positiveSlotLength = abs(slotLength);
        if(positiveSlotLength == 0) {return 0; }
        size_t numOfPages = fileHandle.getNumberOfPages();
        bool isPageFound = false;
        char *pageData;
        void *slotTable;
        int offset4NewRecord = 0, slotTableLen = 0;
        int freeSpc = -1;
        for (int i = 0; i < numOfPages; i++) {
            pageData = (char *) malloc(PAGE_SIZE);
            fileHandle.readPage(i, pageData);
            slotTableLen = getSlotTableLength(pageData);
//          (outdated)end of a page: freeSpace + '$$' + slottable; new page - end: freeSpc + _ _
//          (new)end of a page: slottable + slottable len + freeSpace; new page - end: slottable + slottable len + freeSpc
            freeSpc = getFreeSpc(pageData);
            if (freeSpc >= positiveSlotLength) {
//                rid.pageNum = i;
                slotTable = (char *) malloc(slotTableLen + 2 * INT_FIELD_LEN);
                memcpy(slotTable, (char *) pageData + PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN, slotTableLen); //old slot table
                //find empty slot
                int idx = 0;
                for (idx = 0; idx < slotTableLen / (2 * SLOT_TABLE_FIELD_LEN); idx++) {
                    int tempOffset = 0, tempLength = 0;
                    getOffsetAndLengthUsingSlotNum(idx, pageData, slotTableLen, tempOffset, tempLength);
//                    getOffAndLengWithSlotNumInSlotTable(idx, slotTable, tempOffset, tempLength);
                    if (tempOffset == 0 && tempLength == 0) {
                        isPageFound = true;
                        rid.slotNum = idx;
//                      update slot table
                        offset4NewRecord = PAGE_SIZE - slotTableLen - freeSpc - 2 * INT_FIELD_LEN;
                        freeSpc -= positiveSlotLength;
                        break;
                    }
                }
                if (!isPageFound && (2 * SLOT_TABLE_FIELD_LEN) + positiveSlotLength <= freeSpc) { // no empty slot
                    rid.slotNum = idx; //slotTableLen / (SLOT_TABLE_FIELD_LEN * 2);
                    isPageFound = true;
                    offset4NewRecord = PAGE_SIZE - slotTableLen - freeSpc - 2 * INT_FIELD_LEN;
                    slotTableLen += (2 * SLOT_TABLE_FIELD_LEN);
                    freeSpc = freeSpc - positiveSlotLength - (2 * SLOT_TABLE_FIELD_LEN);
                }
                if (isPageFound) {
                    rid.pageNum = i;
                    updateSlotTableWithSlotNum((char*)slotTable, rid.slotNum, slotLength, offset4NewRecord);
                    memcpy((char *) pageData + offset4NewRecord, encodedData, positiveSlotLength);   //add new record
                    addPageTail(pageData, freeSpc, (char*)slotTable, slotTableLen);
                    fileHandle.writePage(i, pageData);
                    free(pageData);
                    free(slotTable);
                    break;
                }
                free(slotTable);
            }
            free(pageData);
        }
        if (!isPageFound) { //new Page
            slotTableLen = 2 * SLOT_TABLE_FIELD_LEN;
            rid.pageNum = numOfPages;
            rid.slotNum = 0;
            freeSpc = PAGE_SIZE - (2 * SLOT_TABLE_FIELD_LEN) - 2 * INT_FIELD_LEN - positiveSlotLength;
            pageData = (char *) malloc(PAGE_SIZE);
            memcpy((void *) pageData, encodedData, positiveSlotLength);
            slotTable = malloc(2 * SLOT_TABLE_FIELD_LEN);
//            memcpy((char *) slotTable, &initialOffset, SLOT_TABLE_FIELD_LEN);
//            memcpy((char *) slotTable + SLOT_TABLE_FIELD_LEN, &slotLength, SLOT_TABLE_FIELD_LEN);
            updateSlotTableWithSlotNum((char*)slotTable, rid.slotNum, slotLength, 0);
            addPageTail(pageData, freeSpc, (char*)slotTable, slotTableLen);
            fileHandle.appendPage(pageData);
            free(pageData);
            free(slotTable);
        }
        return 0;
    }

    void RecordBasedFileManager::updateSlotTableWithSlotNum(void *slotTable, int slotNum, int slotLength,
                                                            int offset4NewRecord) const {
        memcpy((char *) slotTable + slotNum * INT_FIELD_LEN, &offset4NewRecord, SLOT_TABLE_FIELD_LEN);
        memcpy((char *) slotTable + slotNum * INT_FIELD_LEN + SLOT_TABLE_FIELD_LEN, &slotLength, SLOT_TABLE_FIELD_LEN);
    }

    void RecordBasedFileManager::addPageTail(void *pageData, int freeSpc, char *slotTable, int slotTableLen) const {
        memcpy((char *) pageData + PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN, slotTable, slotTableLen);
        updateFreeSpc(pageData, freeSpc);
        updateSlotTableLen(pageData, slotTableLen);
//        memcpy((char *) pageData + PAGE_SIZE - INT_FIELD_LEN, &freeSpc, INT_FIELD_LEN);
//        memcpy((char *) pageData + PAGE_SIZE - 2 * INT_FIELD_LEN, &slotTableLen, INT_FIELD_LEN);
    }

/**
 * Given the slot number, search the slot table to get the offset and the length of the record
 * for recorded inserted from other page (due to update):
 *      offset >= 0, length < 0. length = - realLen.
 * In this function, we get the length of the record.
 * returned length may < 0!!!!
 * @param slotNum
 * @param pageData
 * @param slotTableLen
 * @param offset
 * @param length
 */
    void RecordBasedFileManager::getOffsetAndLengthUsingSlotNum(const int slotNum, const void *pageData, int slotTableLen,
                                                                int &offset, int &length) const {
        memcpy(&offset, (char *) pageData + PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN + 2 * SLOT_TABLE_FIELD_LEN * slotNum, SLOT_TABLE_FIELD_LEN); //offset
        memcpy(&length, (char *) pageData + PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN + 2 * SLOT_TABLE_FIELD_LEN * slotNum + SLOT_TABLE_FIELD_LEN, SLOT_TABLE_FIELD_LEN);  //length
    }

//    void RecordBasedFileManager::getOffAndLengWithSlotNumInSlotTable(const int slotNum, const void *slotTable,
//                                                                     int &offset, int &length) const {
//        memcpy(&offset, (char *) slotTable + 2 * SLOT_TABLE_FIELD_LEN * slotNum, SLOT_TABLE_FIELD_LEN); //offset
//        memcpy(&length, (char *) slotTable + 2 * SLOT_TABLE_FIELD_LEN * slotNum + SLOT_TABLE_FIELD_LEN, SLOT_TABLE_FIELD_LEN);  //length
//    }

    /**
 * get the the last index(th) integer in the current page (pageData);
     * idx = -1 for free space; idx = -2 for slot table length
 * @param pageData
 * @return
 */
    int RecordBasedFileManager::getLastInteger(void *pageData, int index) {
        int slotTableLen = 0;
        memcpy(&slotTableLen, (char*) pageData + PAGE_SIZE + index * INT_FIELD_LEN, INT_FIELD_LEN);
        return slotTableLen;
    }

    int RecordBasedFileManager::getFreeSpc(void *pageData) {return getLastInteger(pageData, -1); }
    int RecordBasedFileManager::getSlotTableLength(void *pageData) { return getLastInteger(pageData, -2); }

//    int RecordBasedFileManager::getSlotTableLength(void *pageData) {
//        int slotTableLen = 0;
//        memcpy(&slotTableLen, (char*) pageData + PAGE_SIZE - 2 * INT_FIELD_LEN, INT_FIELD_LEN);
//        return slotTableLen;
//    }
//
//    int RecordBasedFileManager::getFreeSpc(char *pageData) {
//        int freeSpc = -1;
//        memcpy(&freeSpc, pageData + PAGE_SIZE - sizeof(int), sizeof(int));
//        return freeSpc;
//    }



    int RecordBasedFileManager::encodeRecordData_returnSlotLength(const std::vector<Attribute> &recordDescriptor,
                                                                  const void *data, void *encodedData) {
        int attrNum = recordDescriptor.size(); //NUM of attributes
        int nullIndicatorNum = ceil((double) attrNum / CHAR_BIT);
        char *nullIndicatorStr = (char *) malloc(nullIndicatorNum);
        memcpy(nullIndicatorStr, data, nullIndicatorNum);
        bool isFieldNull = false;
        int offset = 0;
        int varcharLen = 0;
        int dataOffset = nullIndicatorNum;
        int sizeFour = sizeof(int);
        int nullFieldLen = -1;
        for (int i = 0; i < attrNum; i++) {
            //1: null; 0: not null
            isFieldNull = nullIndicatorStr[i / CHAR_BIT] & (1 << (7 - (i % CHAR_BIT)));
            if (!isFieldNull) {
                if ((recordDescriptor[i].type == TypeInt) || recordDescriptor[i].type == TypeReal) {
                    memcpy((char *) encodedData + offset, &sizeFour, sizeFour);
                    offset += sizeFour;
                    memcpy((char *) encodedData + offset, (char *) data + dataOffset, recordDescriptor[i].length);
                    offset += recordDescriptor[i].length;
                    dataOffset += recordDescriptor[i].length;
                } else { //  TypeVarChar
                    memcpy(&varcharLen, (char *) data + dataOffset, sizeof(int));
                    memcpy((char *) encodedData + offset, (char *) data + dataOffset, varcharLen + sizeof(int));
                    offset = offset + varcharLen + sizeof(int);
                    dataOffset = dataOffset + varcharLen + sizeof(int);
                }
            } else {
                memcpy((char *) encodedData + offset, &nullFieldLen, sizeof(int));
                offset += sizeof(int);
            }
        }
        free(nullIndicatorStr);
        return offset;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
        void *pageData = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, pageData);
        int slotTableLen = getSlotTableLength(pageData);
        int offset = 0, length = 0;
        void *encodedData;
        void *directedPageData;
        getOffsetAndLengthUsingSlotNum(rid.slotNum, pageData, slotTableLen, offset, length);
        if (offset >= 0 && length < 0) {length = -length; }
        if (offset >= 0 && length > 0) { //record in this page
            encodedData = malloc(PAGE_SIZE);
            memcpy(encodedData, (char *) pageData + offset, length);
        } else if (offset == 0 && length == 0) {
            free(pageData);
//        cout << "Fail to read the record: the record has been deleted." << endl;
            return -1;
        }
//data in other page
//  offset: -page id -1; //avoid offset = 0
//  length: -slot id - 1. //avoid slot id = 0
        else if (offset < 0 && length < 0) {
            // todo
            RID directedRid;
            getRealDirectedPageNumAndSlotNum(offset, length, directedRid);
            int directedOffset = 0, directedLength = 0;
            directedPageData = malloc(PAGE_SIZE);
            fileHandle.readPage(directedRid.pageNum, directedPageData);
//        cout<<"page data: "<<endl;
//        printStr(PAGE_SIZE, (char*)directedPageData);
            int directedSlotTableLen = getSlotTableLength(directedPageData);
//        printSlotTable(directedPageData, 100);
            getOffsetAndLengthUsingSlotNum(directedRid.slotNum, directedPageData, directedSlotTableLen, directedOffset,directedLength);
            if (directedLength < 0) {//actually it is definitely < 0
                directedLength = -directedLength;
            }
            encodedData = malloc(PAGE_SIZE);
            memcpy(encodedData, (char *) directedPageData + directedOffset, directedLength);
            free(directedPageData);
        }
        free(pageData);
//    cout<<"in read: encoded data = ";
//    printEncodedRecord(recordDescriptor, encodedData);
//    cout<<"======"<<endl;
        decodeData(recordDescriptor, data, encodedData);
        free(encodedData);

        return 0;
    }

    void RecordBasedFileManager::getRealDirectedPageNumAndSlotNum(int offset, int length, RID &directedRid) const {
        directedRid.pageNum = -(1 + offset);
        directedRid.slotNum = -(1 + length);
    }

    int RecordBasedFileManager::decodeData(const std::vector<Attribute> &recordDescriptor, void *data, const void *encodedData) {
        int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
        char* nullFieldsIndicator = (char *) malloc(nullFieldsIndicatorActualSize);
        memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
        int fieldLen = 0;
        int encodedDataOffset = 0, offset = nullFieldsIndicatorActualSize;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            memcpy(&fieldLen, (char *) encodedData + encodedDataOffset, sizeof(int));
            encodedDataOffset += sizeof(int);
            if (fieldLen == -1) {
                nullFieldsIndicator[i / 8] = nullFieldsIndicator[i / 8] | (1 << (7 - (i % 8)));
            } else {
                if(recordDescriptor[i].type == TypeVarChar){
                    memcpy((char*) data + offset, &fieldLen, sizeof(int));
                    offset += sizeof(int);
                }
                memcpy((char *) data + offset, (char *) encodedData + encodedDataOffset, fieldLen);
                offset += fieldLen;
                encodedDataOffset += fieldLen;
            }
        }
        memcpy((char *) data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
        free(nullFieldsIndicator);
    }


    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid) {
        void *oldPageData = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, oldPageData);
        int slotTableLen = getSlotTableLength(oldPageData);
        if (slotTableLen <= 0) {return -1; }//cout << "Fail to delete the record: the length of the slot table <=0. " << endl;
        int offset = 0, length = 0;
        getOffsetAndLengthUsingSlotNum(rid.slotNum, oldPageData, slotTableLen, offset, length); // get the offset and the length of the deleted slot
//      directed: slot info in slot table:
//      offset: -page id -1; //avoid offset = 0
//      length: -slot id - 1. //avoid slot id = 0
//      old page: offset < 0, length < 0: direct to other page
//      directed page: offset >= 0, length < 0: the record is an updated record from other page, so no need to count for this page
        if (offset >= 0 && length < 0) {length = -length; }  //cout << "Fail to delete: this record does not belong to page " << rid.pageNum << endl;
        if (offset == 0 && length == 0) {return -1; }  //cout << "Fail to delete: the record has already been deleted" << endl;
        void *newPageData = malloc(PAGE_SIZE);
//      delete the record
        if (offset >= 0 && length > 0) {//record in the current page
            //  update the slot table:
            //      1. set the deleted record's <offset, length> to <0,0>;
            //      2. update the offsets of the latter records
            //小的不变  大于offset的减length
            updateOffsetsInSlotTable(oldPageData, slotTableLen, 0 - length, offset, true);
            //steps: 1. add the former records; 2. add the latter records; 3. add the slot table; 4. update the slot table
            formDataPageAfterUpdateOrDelete(newPageData, oldPageData, (char*)"", slotTableLen, offset, length, 0);
//            formDataPageAfterDelete(oldPageData, slotTableLen, offset, length, newPageData);
        } else if (offset < 0 && length < 0) { //data in other page
//      directed: slot info in slot table:
//          offset: -page id -1; //avoid offset = 0
//          length: -slot id - 1. //avoid slot id = 0
//      old page: offset < 0, length < 0: direct to other page
//      directed page: offset >= 0, length < 0: the record is an updated record from other page, so no need to count for this page
            RID directedRid;
            getRealDirectedPageNumAndSlotNum(offset, length, directedRid);
            deleteRecord(fileHandle, recordDescriptor, directedRid);
//        update the slot information in the new page (not new directed page)
            memcpy(newPageData, oldPageData, PAGE_SIZE);
            updateSlotTable_SetOffsetAndLengthBySlotNum(newPageData, slotTableLen, rid.slotNum, 0, 0);
        }
        fileHandle.writePage(rid.pageNum, newPageData);
        free(oldPageData);
        free(newPageData);
        return 0;
    }

    /**
 * move the latter records to the left or right. length > 0: right; length < 0: left
    directed: slot info in slot table:
      offset: -page id -1; //avoid offset = 0
      moveLength: -slot id - 1. //avoid slot id = 0
      old page: offset < 0, moveLength < 0: direct to other page
      directed page: offset >= 0, moveLength < 0: the record is an updated record from other page,
              so no need to count for this page
    slot table info 根据Page data 分以下几种
    1. 正常insert update的，offset >= 0, moveLength >=0
    2. 当前page装不下了，存到其他页的，offset < 0, moveLength < 0
    3. After delete, offset == 0, moveLength ==0
    4. 别的page存过来的: offset >=0, moveLength < 0
 * @param pageData
 * @param slotTableLen
 * @param offset
 * @param moveLength
 */
    void RecordBasedFileManager::updateOffsetsInSlotTable(const void *pageData, int slotTableLen, int moveLength, int offset, bool isDelete) const {
        int tempOffset = 0, tempLength = 0;
        for (int i = 0; i < slotTableLen / (SLOT_TABLE_FIELD_LEN * 2); i++) {
            getOffsetAndLengthUsingSlotNum(i, pageData, slotTableLen, tempOffset, tempLength);
            if (tempOffset < 0 && tempLength < 0) {continue; } //directed to other page, no influence on the current page
            if (tempOffset == 0 && tempLength == 0) {continue; } //after delete
            int moveLen4Update = 0;
            //下面两种情况包含分类中的1, 2
            if (tempOffset > offset) {
                tempOffset = tempOffset + moveLength;
                updateOffsetUsingSlotNumber(pageData, slotTableLen, tempOffset, i);
            } else if (tempOffset == offset) {
                if (isDelete) {//set the deleted record slot information to <0,0>
                    updateSlotTable_SetOffsetAndLengthBySlotNum((char*)pageData, slotTableLen, i, 0, 0);
                } else {//update
                    moveLen4Update = tempLength + moveLength;
                    if(tempLength < 0){moveLen4Update = tempLength - moveLength; }
                    updateLengthUsingSlotNumber((char*)pageData, slotTableLen, moveLen4Update, i);
                }
            }
        }
    }

/**
 *
 * @param pageData
 * @param slotTableLen
 * @param slotNum
 * @param a
 * @param b
 */
    void
    RecordBasedFileManager::updateSlotTable_SetOffsetAndLengthBySlotNum(void *pageData, int slotTableLen, int slotNum, int offset, int length) const {
        updateOffsetUsingSlotNumber(pageData, slotTableLen, offset, slotNum);
        updateLengthUsingSlotNumber(pageData, slotTableLen, length, slotNum);
    }

    int RecordBasedFileManager::updateOffsetUsingSlotNumber(const void *pageData, int slotTableLen, int tempOffset, int slotNum) const {
        memcpy((char *) pageData + PAGE_SIZE - 2 * INT_FIELD_LEN - slotTableLen + 2 * SLOT_TABLE_FIELD_LEN * slotNum, &tempOffset, SLOT_TABLE_FIELD_LEN); //offset
        return 0;
    }

    int RecordBasedFileManager::updateLengthUsingSlotNumber(void *pageData, int slotTableLen, int length,
                                                            int slotNum) const {
        memcpy((char *) pageData + PAGE_SIZE - 2 * INT_FIELD_LEN - slotTableLen + 2 * SLOT_TABLE_FIELD_LEN * slotNum + SLOT_TABLE_FIELD_LEN,
               &length, SLOT_TABLE_FIELD_LEN); //length
        return 0;
    }

    void RecordBasedFileManager::updateFreeSpc(void *newPageData, int freeSpc) const {
        updateLastInteger(newPageData, freeSpc, -1);
    }
    void RecordBasedFileManager::updateSlotTableLen(void *newPageData, int slotTableLen) const {
        updateLastInteger(newPageData, slotTableLen, -2);
    }

    void RecordBasedFileManager::updateLastInteger(void *newPageData, int val, int index) const {
        memcpy((char *) newPageData + PAGE_SIZE + index * INT_FIELD_LEN, &val, INT_FIELD_LEN);
    }

    int RecordBasedFileManager::getNullIndicatorStr(const vector<Attribute> &recordDescriptor, const void *data,
                                                    unsigned char *&nullIndicatorStr) const {
        int nullIndicatorNum= ceil(1.0 * recordDescriptor.size() / CHAR_BIT);
        int attrNum = recordDescriptor.size(); //NUM of attributes
        nullIndicatorStr = (unsigned char*)malloc(nullIndicatorNum);
        memset(nullIndicatorStr, 0, nullIndicatorNum);
        memcpy(nullIndicatorStr, data, nullIndicatorNum);
        return nullIndicatorNum;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        unsigned char* nullIndicatorStr;// = (unsigned char*)malloc(1);
        int offset = getNullIndicatorStr(recordDescriptor, data, nullIndicatorStr);

        for(int i=0;i<recordDescriptor.size();i++){
            bool isFieldNull = nullIndicatorStr[i/8] & (1 << (7-(i%8)));
            if(!isFieldNull){
                if(recordDescriptor[i].type == TypeInt){
                    int int_attr;
                    memcpy(&int_attr,(char*)data+offset,recordDescriptor[i].length);
                    offset += recordDescriptor[i].length;
                    out<<recordDescriptor[i].name << ": " << int_attr;
                }
                else if(recordDescriptor[i].type == TypeReal){
                    float floatValue = 0;
                    memcpy(&floatValue,(char*)data+offset,recordDescriptor[i].length);
                    offset += recordDescriptor[i].length;
                    out << recordDescriptor[i].name << ": " << floatValue;
                }else { //  TypeVarChar
                    int varcharLen = 0;
                    memcpy(&varcharLen, (char *)data + offset, sizeof(int));
                    char* strValue = (char*) malloc(varcharLen);
                    offset += sizeof(int);
                    memcpy(strValue,(char*)data+offset,varcharLen);
                    offset += varcharLen;
                    out <<"    "<<recordDescriptor[i].name << ": " ;//
                    printStr(varcharLen, strValue, out);
//                cout<<endl;
                    free(strValue);
                }
            }
            else{out << recordDescriptor[i].name << ": NULL"; }
            if(i < recordDescriptor.size() - 1) {out<<", ";}
        }
        out<<endl;
        free(nullIndicatorStr);
        return 0;
    }

    void RecordBasedFileManager::printStr(int varcharLen, const char *strValue, std::ostream &out) const {
    for(int i = 0; i < varcharLen; i++){
        out<<*(char*)(strValue + i);
    }
}

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        char *pageData = (char *) malloc(PAGE_SIZE);
        char *newPageData = (char *) malloc(PAGE_SIZE);
        if(fileHandle.readPage(rid.pageNum, pageData) != 0) { std::cout<<"Fail to read the page."<<std::endl; return -1; }
        int slotTableLen = getSlotTableLength(pageData);
        int offset = 0, length = 0;
        getOffsetAndLengthUsingSlotNum(rid.slotNum, pageData, slotTableLen, offset, length);
        if(offset < 0 && length < 0) {
            RID directedPageRid;
            getRealDirectedPageNumAndSlotNum(offset, length, directedPageRid);
            int res = updateRecord(fileHandle, recordDescriptor, data, directedPageRid);
            if(res == -6){insertRecordInDirectedPageAndSetDirSlot(fileHandle, recordDescriptor, data, rid, newPageData);
            }else {directed_setSlotForOldPage(fileHandle, rid, newPageData, directedPageRid); }
        }
        else { //if(offset >= 0 && length < 0 || offset >= 0 && length > 0)
            void *encodedData = malloc(PAGE_SIZE);
            void *oldEncodedRecord = malloc(PAGE_SIZE);
            void *oldRecord = malloc(PAGE_SIZE);
            readRecord(fileHandle, recordDescriptor, rid, oldRecord);
            int newSlotLength = encodeRecordData_returnSlotLength(recordDescriptor, data, encodedData);
            int oldSlotLength = encodeRecordData_returnSlotLength(recordDescriptor, oldRecord, oldEncodedRecord);
            int freeSpc = getFreeSpc(pageData);
            free(oldEncodedRecord);

            if(freeSpc >= newSlotLength - oldSlotLength){ // update on the current page
                if (newSlotLength != oldSlotLength) { //conditon 1, 2
                    updateOffsetsInSlotTable(pageData, slotTableLen, newSlotLength - oldSlotLength, offset, false);
                }
                formDataPageAfterUpdateOrDelete(newPageData, pageData, encodedData, slotTableLen, offset, length, newSlotLength);
            } else {//update on the directed page
                deleteRecord(fileHandle, recordDescriptor, rid);
                if(offset >= 0 && length < 0) {
                    free(newPageData);
                    free(oldRecord);
                    free(pageData);
                    return -6; //在外层更新
                }else {
                    insertRecordInDirectedPageAndSetDirSlot(fileHandle, recordDescriptor, data, rid, newPageData);
                }
            }
            free(oldRecord);
        }
        fileHandle.writePage(rid.pageNum, newPageData);
        free(newPageData);
        free(pageData);
        return 0;
    }

    /**
     * to get the new page data after delete
     * @param pageData -- original data
     * @param slotTableLen -- length of the slot table in the original page
     * @param offset -- the offset of the deleted slot
     * @param length -- the length of the deleted slot
     * @param newPageData
     * length > 0
     */
//    void RecordBasedFileManager::formDataPageAfterDelete(void *pageData, int slotTableLen,
//                                                         int offset, int length, const void *newPageData) {
//        length = abs(length);
//        int freeSpc = getFreeSpc(pageData);
//        memcpy((char *) newPageData, pageData, offset);
//        int latterRecords = PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN - freeSpc - offset - length;
//        memcpy((char *) newPageData + offset, (char *) pageData + offset + length, latterRecords);
//        freeSpc += length;
//        updateFreeSpc((char*)newPageData, freeSpc);
//        updateSlotTableLen((char*)newPageData, slotTableLen);
//    }

    void RecordBasedFileManager::formDataPageAfterUpdateOrDelete(void *newPageData, void *pageData,
            const void *data, int slotTableLen, int offset, int length, int newSlotLength) {
        int freeSpc = getFreeSpc(pageData);
        if(length < 0) {length = - length; }
        memcpy((char *) newPageData, pageData, offset);
        memcpy((char *) newPageData + offset, data, newSlotLength);
        int latterRecords = PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN - freeSpc - offset - length;
        memcpy((char *) newPageData + offset + newSlotLength, (char*)pageData + offset + length, latterRecords);
//    add free space
        freeSpc += (length - newSlotLength);
        memcpy((char*)newPageData + PAGE_SIZE - slotTableLen - 2 - sizeof(int), &freeSpc, sizeof(int));
    }

    void RecordBasedFileManager::directed_setSlotForOldPage(FileHandle &fileHandle, const RID &rid, char *newPageData, RID newDirectedRid) {
        int newOffset = 0, newLength = 0;
        computeTheDirectedOffsetAndLengthInTheOldPage(newDirectedRid, newOffset, newLength);
//      3. update slot table of the old page with rid
        fileHandle.readPage(rid.pageNum, newPageData);
        updateSlotTable_SetOffsetAndLengthBySlotNum(newPageData, getSlotTableLength(newPageData), rid.slotNum, newOffset, newLength);
    }

    /**
 *offset: -page id -1; //avoid offset = 0
 * length: -slot id - 1. //avoid slot id = 0
 * @param directedRid
 * @param newOffset
 * @param newLength
 */
    void RecordBasedFileManager::computeTheDirectedOffsetAndLengthInTheOldPage(const RID &directedRid, int &newOffset,
                                                                               int &newLength) const {
        newOffset = -directedRid.pageNum - 1;
        newLength = -directedRid.slotNum - 1;
    }

    void RecordBasedFileManager::insertRecordInDirectedPageAndSetDirSlot(FileHandle &fileHandle,
                                                                         const vector<Attribute> &recordDescriptor, const void *data, const RID &rid, char *newPageData) {
//using the directedRid to get the rid in the old page: set <offset, length> to <-directedPageNum-1, -directedSlotNum-1>
        RID newDirectedRid;
        insertRecord(fileHandle, recordDescriptor, data, newDirectedRid);
        setLenToNegLenInDirectedPage(fileHandle, newDirectedRid);
        directed_setSlotForOldPage(fileHandle, rid, newPageData, newDirectedRid);
    }


    RC RecordBasedFileManager::setLenToNegLenInDirectedPage(FileHandle &fileHandle, const RID &directedPageRid) {
        void *directedPageData = (char *) malloc(PAGE_SIZE);
        if(fileHandle.readPage(directedPageRid.pageNum, directedPageData) != 0) {return -1; }//cout<<"Fail to read the directed page."<<endl;
        int slotTableLen2 = getSlotTableLength(directedPageData);
        int directedOffset = 0, directedLength = 0;
        getOffsetAndLengthUsingSlotNum(directedPageRid.slotNum, directedPageData, slotTableLen2,directedOffset, directedLength);
        if(directedLength > 0) {directedLength = - directedLength; }
        updateLengthUsingSlotNumber(directedPageData, slotTableLen2, directedLength, directedPageRid.slotNum);
        fileHandle.writePage(directedPageRid.pageNum, directedPageData);
        free(directedPageData);
    }


    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        void *record = malloc(PAGE_SIZE);
        readRecord(fileHandle, recordDescriptor, rid, record);
        void *encodedRecord = malloc(PAGE_SIZE);
        encodeRecordData_returnSlotLength(recordDescriptor, record, encodedRecord);

        int offset = 0;
        int fieldLen = 0;
        char* nullFieldsIndicator = (char *) malloc(1);
        memset(nullFieldsIndicator, 0, 1);

//    int nullIndicator = -1;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            memcpy(&fieldLen, (char *) encodedRecord + offset, sizeof(int));
            offset += sizeof(int);
            if (isAttrFound(attributeName, i, recordDescriptor)) {
                if (fieldLen == -1) { //null
                    nullFieldsIndicator[0] = nullFieldsIndicator[0] | (1 << 7);
                    memcpy(data, nullFieldsIndicator, 1);
                } else{
                    memcpy(data, nullFieldsIndicator, 1);
                    if (recordDescriptor[i].type == TypeInt ) {
                        int int_attr;
                        memcpy(&int_attr, (char *) encodedRecord + offset, recordDescriptor[i].length);
                        memcpy((char*)data + 1, &int_attr, sizeof(int));
                    } else if (recordDescriptor[i].type == TypeReal) {
                        float floatValue = 0.00;
                        memcpy(&floatValue, (char *) encodedRecord + offset, recordDescriptor[i].length);
                        memcpy((char*)data + 1, &floatValue, sizeof(float));
                    }else{
                        memcpy((char*)data + 1, &fieldLen, sizeof(int));
                        memcpy((char*)data + 1 + sizeof(int), (char*)encodedRecord + offset, fieldLen);
                    }
                }
                break;
            } else {
                if(fieldLen != -1){
                    offset += fieldLen;
                }
            }
        }
        free(record);
        free(encodedRecord);
        return 0;
    }


    bool RecordBasedFileManager::isAttrFound(const string &filterAttributeName, int i_recordDescriptorCounter,
                                             const vector<Attribute> &recordDescriptor) const {
        return strcmp(filterAttributeName.c_str(), recordDescriptor[i_recordDescriptorCounter].name.c_str()) == 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

