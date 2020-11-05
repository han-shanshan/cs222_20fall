#include <iostream>
#include "src/include/rbfm.h"
//#include "cmath"
#include <math.h>
#include <vector>
#include <sstream>

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
        char encodedData[PAGE_SIZE];
        int slotLength = encodeRecordData_returnSlotLength(recordDescriptor, data, encodedData);
        if (fileHandle.file == NULL) {return -1; } //error;
//    cout<<"slotlength = "<<slotLength<<endl;
        int res = insertEncodedRecord(fileHandle, rid, encodedData, slotLength);
        if(res != 0 ){return -1;}
        return 0;
    }

    RC RecordBasedFileManager::insertEncodedRecord(FileHandle &fileHandle, RID &rid, const void *encodedData,
                                                   int slotLength) {
        int positiveSlotLength = abs(slotLength);
        if(positiveSlotLength == 0) {return 0; }
        size_t numOfPages = fileHandle.getNumberOfPages();
        bool isPageFound = false;
        char pageData[PAGE_SIZE];
        void *slotTable;
        char *oldSlotTable;
        int offset4NewRecord = 0, slotTableLen = 0;
        int freeSpc = -1;
        for (int i = 0; i < numOfPages; i++) {
//            pageData = (char *) malloc(PAGE_SIZE);
            fileHandle.readPage(i, pageData);
            int slotTableLen = getSlotTableLength(pageData);
//          end of a page: slottable + slottable len + freeSpace; new page - end: slottable + slottable len + freeSpc
            freeSpc = getFreeSpc(pageData);
            if (freeSpc >= positiveSlotLength) {
                rid.pageNum = i;
                oldSlotTable = (char *) malloc(slotTableLen + 2 * INT_FIELD_LEN);
                memcpy(oldSlotTable, (char *) pageData + PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN, slotTableLen); //old slot table
                //find empty slot
                for (int j = 0; j < slotTableLen / (2 * SLOT_TABLE_FIELD_LEN); j++) {
                    int tempOffset = 0, tempLength = 0;
                    getOffsetAndLengthUsingSlotNum(j, pageData, slotTableLen, tempOffset, tempLength);
                    if (tempOffset == 0 && tempLength == 0) {
                        isPageFound = true;
                        rid.slotNum = j;
                        slotTable = (char *) malloc(slotTableLen);
                        memcpy((char *) slotTable, oldSlotTable, slotTableLen);//old Slot Table
//                  update slot table
                        offset4NewRecord = PAGE_SIZE - slotTableLen - freeSpc - 2 * INT_FIELD_LEN;
                        memcpy((char *) slotTable + j * 2 * SLOT_TABLE_FIELD_LEN, &offset4NewRecord,
                               SLOT_TABLE_FIELD_LEN);
                        memcpy((char *) slotTable + j * 2 * SLOT_TABLE_FIELD_LEN + SLOT_TABLE_FIELD_LEN, &slotLength,
                               SLOT_TABLE_FIELD_LEN);
                        freeSpc -= positiveSlotLength;
                        break;
                    }
                }
                //没有空的slot
                if (!isPageFound && (2 * SLOT_TABLE_FIELD_LEN) + positiveSlotLength <= freeSpc) {
                    rid.slotNum = slotTableLen / (SLOT_TABLE_FIELD_LEN * 2);
                    isPageFound = true;
                    slotTable = (char *) malloc(slotTableLen + 2 * (SLOT_TABLE_FIELD_LEN));
                    memcpy((char *) slotTable, oldSlotTable, slotTableLen);//old Slot Table
                    offset4NewRecord = PAGE_SIZE - slotTableLen - freeSpc - 2 * INT_FIELD_LEN;
                    memcpy((char *) slotTable + slotTableLen, &offset4NewRecord, SLOT_TABLE_FIELD_LEN);
                    memcpy((char *) slotTable + slotTableLen + SLOT_TABLE_FIELD_LEN, &slotLength, SLOT_TABLE_FIELD_LEN);
                    slotTableLen = slotTableLen + (2 * SLOT_TABLE_FIELD_LEN);
                    freeSpc = freeSpc - positiveSlotLength - (2 * SLOT_TABLE_FIELD_LEN);
                }
                if (isPageFound) {
                    rid.pageNum = i;
                    memcpy((char *) pageData + offset4NewRecord, encodedData, positiveSlotLength);   //add new record
                    memcpy((char *) pageData + PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN, slotTable, slotTableLen);
                    memcpy((char *) pageData + PAGE_SIZE - INT_FIELD_LEN , &freeSpc, sizeof(int));
                    memcpy((char *) pageData + PAGE_SIZE - 2 * INT_FIELD_LEN , &slotTableLen, sizeof(int));
                    fileHandle.writePage(i, pageData);
//                    free(pageData);
                    free(slotTable);
                    break;
                }
//                free(slotTable);////////////////////
            }
//            free(pageData);
        }

        if (!isPageFound) { //new Page
            int initialOffset = 0;
            rid.pageNum = numOfPages;
            rid.slotNum = 0;
            freeSpc = PAGE_SIZE - (2 * SLOT_TABLE_FIELD_LEN) - 2 * sizeof(int) - positiveSlotLength;
//            memset(pageData, 0, PAGE_SIZE);
            memcpy((void *) pageData, encodedData, positiveSlotLength);
            slotTable = malloc(2 * SLOT_TABLE_FIELD_LEN);
            slotTableLen = 2 * SLOT_TABLE_FIELD_LEN;
            memcpy((char *) slotTable, &initialOffset, SLOT_TABLE_FIELD_LEN);
            memcpy((char *) slotTable + SLOT_TABLE_FIELD_LEN, &slotLength, SLOT_TABLE_FIELD_LEN);
            memcpy((char *) pageData + PAGE_SIZE - (4 * SLOT_TABLE_FIELD_LEN), slotTable, 2 * SLOT_TABLE_FIELD_LEN);
            memcpy((char *)pageData + PAGE_SIZE - (SLOT_TABLE_FIELD_LEN), &freeSpc, sizeof(int));
            memcpy((char *)pageData + PAGE_SIZE - (2 * SLOT_TABLE_FIELD_LEN), &slotTableLen, sizeof(int));
            fileHandle.appendPage(pageData);
//            free(pageData);
            free(slotTable);
        }
        return 0;
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

    /**
 * get the length of the slot table in the current page (pageData)
 * The '$$' is not calculated
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

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        char pageData[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, pageData);
        int slotTableLen = getSlotTableLength(pageData);
        int offset = 0, length = 0;
        char encodedData[PAGE_SIZE];
        getOffsetAndLengthUsingSlotNum(rid.slotNum, pageData, slotTableLen, offset, length);
        if (offset >= 0 && length < 0) {length = -length; }
        if (offset >= 0 && length > 0) { //record in this page
            memcpy(encodedData, (char *) pageData + offset, length);
        } else if (offset == 0 && length == 0) {
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
            char directedPageData[PAGE_SIZE];
            fileHandle.readPage(directedRid.pageNum, directedPageData);
//        cout<<"page data: "<<endl;
//        printStr(PAGE_SIZE, (char*)directedPageData);
            int directedSlotTableLen = getSlotTableLength(directedPageData);
//        printSlotTable(directedPageData, 100);
            getOffsetAndLengthUsingSlotNum(directedRid.slotNum, directedPageData,
                                           directedSlotTableLen, directedOffset,directedLength);
            if (directedLength < 0) {//actually it is definitely < 0
                directedLength = -directedLength;
            }
            memcpy(encodedData, (char *) directedPageData + directedOffset, directedLength);
        }
        decodeData(recordDescriptor, data, encodedData);

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
        char oldPageData[PAGE_SIZE];
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
        char newPageData[PAGE_SIZE];
//      delete the record
        if (offset >= 0 && length > 0) {//record in the current page
            //  update the slot table:
            //      1. set the deleted record's <offset, length> to <0,0>;
            //      2. update the offsets of the latter records
            //小的不变  大于offset的减length
            //steps: 1. add the former records; 2. add the latter records; 3. add the slot table; 4. update the slot table
//            formDataPageAfterUpdateOrDelete(newPageData, oldPageData, NULL, slotTableLen, offset, length, 0);
            formDataPageAfterDelete(oldPageData, slotTableLen, offset, length, newPageData);
            updateOffsetsInSlotTable(newPageData, slotTableLen, 0 - length, offset, true);
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
        return 0;
    }
    /**
 * move the latter records to the left or right. length > 0: right; length < 0: left
 * @param pageData
 * @param slotTableLen
 * @param offset
 * @param moveLength
 */
    void RecordBasedFileManager::updateOffsetsInSlotTable(const void *pageData, int slotTableLen,
                                                          int moveLength, int offset, bool isDelete) const {
        int tempOffset = 0, tempLength = 0;
        for (int i = 0; i < slotTableLen / (SLOT_TABLE_FIELD_LEN * 2); i++) {
            getOffsetAndLengthUsingSlotNum(i, pageData, slotTableLen, tempOffset, tempLength);
/*
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
 */
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
                    if(tempLength < 0){
                        moveLen4Update = tempLength - moveLength;
                    }
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
    RecordBasedFileManager::updateSlotTable_SetOffsetAndLengthBySlotNum(void *pageData, int slotTableLen, int slotNum,
                                                                        int offset, int length) const {
        updateOffsetUsingSlotNumber(pageData, slotTableLen, offset, slotNum);
        updateLengthUsingSlotNumber(pageData, slotTableLen, length, slotNum);
    }

    int RecordBasedFileManager::updateOffsetUsingSlotNumber(const void *pageData, int slotTableLen, int tempOffset,
                                                            int slotNum) const {
        memcpy((char *) pageData + PAGE_SIZE - 2 * INT_FIELD_LEN - slotTableLen + 2 * SLOT_TABLE_FIELD_LEN * slotNum, &tempOffset, SLOT_TABLE_FIELD_LEN); //offset
        return 0;
    }

    int RecordBasedFileManager::updateLengthUsingSlotNumber(void *pageData, int slotTableLen, int length,
                                                            int slotNum) const {
        memcpy((char *) pageData + PAGE_SIZE - 2 * INT_FIELD_LEN - slotTableLen + 2 * SLOT_TABLE_FIELD_LEN * slotNum + SLOT_TABLE_FIELD_LEN,
               &length, SLOT_TABLE_FIELD_LEN); //length
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
    void RecordBasedFileManager::formDataPageAfterDelete(void *pageData, int slotTableLen,
                                                         int offset, int length, const void *newPageData) {
        length = abs(length);
        int freeSpc = getFreeSpc((char*)pageData);
        memcpy((char *) newPageData, pageData, offset);
        int latterRecords = PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN - freeSpc - offset - length;
        memcpy((char *) newPageData + offset, (char *) pageData + offset + length, latterRecords);
        freeSpc += length;
        addPageTail((char*)newPageData, freeSpc,
                    (char *) pageData + PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN, slotTableLen);
//        updateFreeSpc((char*)newPageData, freeSpc);
//        updateSlotTableLen((char*)newPageData, slotTableLen);
        //  add the slot table
        memcpy((char *) newPageData + PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN,
               (char *) pageData + PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN, slotTableLen);
    }


    void RecordBasedFileManager::addPageTail(void *pageData, int freeSpc, char *slotTable, int slotTableLen) {
        memcpy((char *) pageData + PAGE_SIZE - slotTableLen - 2 * INT_FIELD_LEN, slotTable, slotTableLen);
        updateFreeSpc(pageData, freeSpc);
        updateSlotTableLen(pageData, slotTableLen);
    }

    void RecordBasedFileManager::updateFreeSpc(void *pageData, int freeSpc)  {
        updateLastInteger(pageData, freeSpc, -1);
    }

    void RecordBasedFileManager::updateSlotTableLen(void *pageData, int len)  {
        updateLastInteger(pageData, len, -2);
    }

    void RecordBasedFileManager::updateLastInteger(void *newPageData, int val, int idx)  {
        memcpy((char*)newPageData + PAGE_SIZE + idx * INT_FIELD_LEN, &val, INT_FIELD_LEN);
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
        char pageData[PAGE_SIZE];
        char newPageData[PAGE_SIZE];
        if(fileHandle.readPage(rid.pageNum, pageData) != 0) { std::cout<<"Fail to read the page."<<std::endl; return -1; }
        int slotTableLen = getSlotTableLength(pageData);
        int offset = 0, length = 0;
        getOffsetAndLengthUsingSlotNum(rid.slotNum, pageData, slotTableLen, offset, length);
        if(offset < 0 && length < 0) {
            RID directedPageRid;
            getRealDirectedPageNumAndSlotNum(offset, length, directedPageRid);
            int res = updateRecord(fileHandle, recordDescriptor, data, directedPageRid);
            if(res == -6){
                insertRecordInDirectedPage(fileHandle, recordDescriptor, data, rid, newPageData);
            }else {directed_setSlotForOldPage(fileHandle, rid, newPageData, directedPageRid); }
        }
        else { //if(offset >= 0 && length < 0 || offset >= 0 && length > 0)
            char encodedData[PAGE_SIZE];
            char oldEncodedRecord[PAGE_SIZE];
            char oldRecord[PAGE_SIZE];
            readRecord(fileHandle, recordDescriptor, rid, oldRecord);
            int newSlotLength = encodeRecordData_returnSlotLength(recordDescriptor, data, encodedData);
            int oldSlotLength = encodeRecordData_returnSlotLength(recordDescriptor, oldRecord, oldEncodedRecord);
            int freeSpc = getFreeSpc(pageData);

            if(freeSpc >= newSlotLength - oldSlotLength){ // update on the current page
                if (newSlotLength != oldSlotLength) { //conditon 1, 2
                    updateOffsetsInSlotTable(pageData, slotTableLen, newSlotLength - oldSlotLength, offset, false);
                }

//                formDataPageAfterUpdateOrDelete(newPageData, pageData, encodedData, slotTableLen, offset, length, newSlotLength);
                formDataPageAfterUpdate(newPageData, pageData, encodedData, slotTableLen, offset, length, newSlotLength,
                                        freeSpc);
            } else {//update on the directed page
                deleteRecord(fileHandle, recordDescriptor, rid);
                if(offset >= 0 && length < 0) {
                    return -6; //在外层更新
                }else {
                    insertRecordInDirectedPage(fileHandle, recordDescriptor, data, rid, newPageData);
                }
            }
        }
        fileHandle.writePage(rid.pageNum, newPageData);
        return 0;
    }


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

    void RecordBasedFileManager::formDataPageAfterUpdate(char *newPageData, const char *pageData,
                                                         const void *data, int slotTableLen, int offset, int length, int newSlotLength, int &freeSpc) {
        if(length < 0) {length = - length; }
        memcpy((char *) newPageData, pageData, offset);
        memcpy((char *) newPageData + offset, data, newSlotLength);
        memcpy((char *) newPageData + offset + newSlotLength, pageData + offset + length,
               PAGE_SIZE - 2 * INT_FIELD_LEN - slotTableLen - freeSpc - offset - length);
        freeSpc = freeSpc - (newSlotLength - length);
        addPageTail((char*)newPageData, freeSpc, (char*)pageData + PAGE_SIZE - 2 * INT_FIELD_LEN - slotTableLen, slotTableLen);
    }

    void RecordBasedFileManager::directed_setSlotForOldPage(FileHandle &fileHandle, const RID &rid, char *newPageData, RID newDirectedRid) {
        int newOffset = 0;
        int newLength = 0;
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

    void RecordBasedFileManager::insertRecordInDirectedPage(FileHandle &fileHandle,
                                                            const vector<Attribute> &recordDescriptor, const void *data, const RID &rid, char *newPageData) {
//using the directedRid to get the rid in the old page: set <offset, length> to <-directedPageNum-1, -directedSlotNum-1>
        RID newDirectedRid;
        insertRecord(fileHandle, recordDescriptor, data, newDirectedRid);
        setLenToNegLenInDirectedPage(fileHandle, newDirectedRid);
        directed_setSlotForOldPage(fileHandle, rid, newPageData, newDirectedRid);
    }


    RC RecordBasedFileManager::setLenToNegLenInDirectedPage(FileHandle &fileHandle, const RID &directedPageRid) {
        char directedPageData[PAGE_SIZE];
        if(fileHandle.readPage(directedPageRid.pageNum, directedPageData) != 0) {return -1; }//cout<<"Fail to read the directed page."<<endl;
        int slotTableLen2 = getSlotTableLength(directedPageData);
        int directedOffset = 0, directedLength = 0;
        getOffsetAndLengthUsingSlotNum(directedPageRid.slotNum, directedPageData, slotTableLen2,directedOffset, directedLength);
        if(directedLength > 0) {directedLength = - directedLength; }
        updateLengthUsingSlotNumber(directedPageData, slotTableLen2, directedLength, directedPageRid.slotNum);
        fileHandle.writePage(directedPageRid.pageNum, directedPageData);
    }


    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        char record[PAGE_SIZE];
        readRecord(fileHandle, recordDescriptor, rid, record);
        char encodedRecord[PAGE_SIZE];
        encodeRecordData_returnSlotLength(recordDescriptor, record, encodedRecord);

        int offset = 0;
        int fieldLen = 0;
        char* nullFieldsIndicator = (char *) malloc(1);
        memset(nullFieldsIndicator, 0, 1);

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
                if(fieldLen != -1){ offset += fieldLen; }
            }
        }
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
        //    Attribute conditionAttributeAttr;
        rbfm_ScanIterator.isIteratorNew = true;
        if(conditionAttribute.length() == 0 ){
            rbfm_ScanIterator.conditionAttributeAttr.length = -1;
            rbfm_ScanIterator.conditionAttributeAttr.name = "";
        }
        else {
            for (int j = 0; j < recordDescriptor.size(); j++) {
                if (strcmp(conditionAttribute.c_str(), recordDescriptor[j].name.c_str()) == 0) {
                    rbfm_ScanIterator.conditionAttributeAttr = recordDescriptor[j];
                }
            }
        }
        for (int i = 0; i < recordDescriptor.size(); i++) {
            if(rbfm_ScanIterator.isDescriptorRequired(attributeNames, recordDescriptor[i].name)){
                rbfm_ScanIterator.selectedRecordDescriptor.push_back(recordDescriptor[i]);
            }
        }
//    this->openFile(fileHandle.getFileName(), rbfm_ScanIterator.iteratorHandle);

        rbfm_ScanIterator.iteratorHandle = fileHandle;
        rbfm_ScanIterator.attributeNames = attributeNames;
        rbfm_ScanIterator.recordDescriptor = recordDescriptor;
        rbfm_ScanIterator.compOp = compOp;
        if(compOp != NO_OP){
            if(rbfm_ScanIterator.conditionAttributeAttr.type == TypeVarChar){
                int varcharLen = 0;
//                cout<<"value:"<<value<<endl;
                memcpy(&varcharLen, value, sizeof(int));
                memcpy(rbfm_ScanIterator.filterValue, value, sizeof(int) + varcharLen);
            }else{
                memcpy(rbfm_ScanIterator.filterValue, value, sizeof(int));
            }
        }

        return 0;
    }



    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        RC readPage_res, read_res = -1;
        // todo: pagenum slot num = 0?
        if(isIteratorNew){
            rid.slotNum = 0;
            rid.pageNum = 0;
            isIteratorNew = false;
            memset(pageData, 0, PAGE_SIZE);
            if(iteratorHandle.readPage(rid.pageNum, pageData) != 0){
                return -1;
            }
            memset(this->pageData, 0, PAGE_SIZE);
            if(getTheCurrentData(rid, this->pageData, rbfm) == 0) {
                memcpy(data, this->pageData, 200);
                return 0;
            }
        }

        while ((rid.pageNum < iteratorHandle.getNumberOfPages())) {
//        cout<<"iteratorHandle.getNumberOfPages = "<<iteratorHandle.getNumberOfPages();
            memset(pageData, 0, PAGE_SIZE);
            readPage_res = iteratorHandle.readPage(rid.pageNum, pageData);
            if (readPage_res != 0) { break; }
            int slotCounter = rbfm.getSlotTableLength(pageData) / (2 * SLOT_TABLE_FIELD_LEN);
//        cout<<", slotcounter = "<<slotCounter<<endl;
            memset(this->pageData, 0, PAGE_SIZE);
            while ((rid.slotNum < slotCounter - 1) && (read_res == -1)) {
                rid.slotNum++;
//            cout<<"print page data111: "<<endl;
//            rbfm.printStr(200, (char*)this->pageData);
                read_res = getTheCurrentData(rid, this->pageData, rbfm);
            }
            if (read_res == 0) {
//            cout<<"print page data: "<<endl;
//            rbfm.printStr(200, (char*)this->pageData);
                memcpy(data, this->pageData, 200);
                break;
            }
            rid.pageNum++;
            rid.slotNum = 0;
        }
        return read_res;
    }


    int
    RBFM_ScanIterator::getTheCurrentData(RID rid, void *data, RecordBasedFileManager &rbfm) {
        memset(notFilteredData, 0, PAGE_SIZE);
        if(rid.pageNum >= iteratorHandle.getNumberOfPages()){
            return -1;
        }
        if(rbfm.readRecord(iteratorHandle, recordDescriptor, rid, notFilteredData) != 0) {
            return -1; //fail to read the record
        }

        //判断是否满足filter条件，如果不满足，则set read_res to -1
        memset(encodedNotFilteredData, 0, PAGE_SIZE);
        rbfm.encodeRecordData_returnSlotLength(recordDescriptor, notFilteredData, encodedNotFilteredData);
//    rbfm.printEncodedRecord(recordDescriptor, encodedNotFilteredData);

        //add filter. 如果没有通过filter则set read_res to -1
        int read_res = 0;
        if (!this->getIsRecordSatisfied(rid, rbfm, iteratorHandle)) { read_res = -1; }
        else{
            int fieldLen = 0;
            int encodedNotFilteredDataOffset = 0;
            int encodedDataOffset = 0;
            memset(encodedFilteredData, 0, PAGE_SIZE);
            for (int i = 0; i < recordDescriptor.size(); i++) {
                memcpy(&fieldLen, (char *) encodedNotFilteredData + encodedNotFilteredDataOffset, sizeof(int));
                encodedNotFilteredDataOffset += sizeof(int);
                if (isDescriptorRequired(attributeNames, recordDescriptor[i].name)) {
                    memcpy((char *) encodedFilteredData + encodedDataOffset, &fieldLen, sizeof(int));

                    encodedDataOffset += sizeof(int);
                    if(fieldLen != -1) { //not null
                        memcpy((char *) encodedFilteredData + encodedDataOffset,
                               (char *) encodedNotFilteredData + encodedNotFilteredDataOffset, fieldLen);
                        encodedDataOffset += fieldLen;
                        encodedNotFilteredDataOffset += fieldLen;
                    }
                } else {
                    if(fieldLen != -1) {
                        encodedNotFilteredDataOffset += fieldLen;
                    }
                }
            }
            std::stringstream stream;
            rbfm.decodeData(selectedRecordDescriptor, data, encodedFilteredData);
        }
        return read_res;
    }



    RC RecordBasedFileManager::printEncodedRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
        int offset = 0;
        int tempInt = 0;
        std::stringstream stream;
        for(int i=0;i<recordDescriptor.size();i++){
            memcpy(&tempInt, (char*)data + offset, sizeof(int));
            offset += sizeof(int);
            if(tempInt != -1){
                if(recordDescriptor[i].type == TypeInt){
                    int int_attr;
                    memcpy(&int_attr,(char*)data+offset,recordDescriptor[i].length);
                    offset += recordDescriptor[i].length;
                    cout <<"    "<<recordDescriptor[i].name << ": " << int_attr;
                }
                else if(recordDescriptor[i].type == TypeReal){
                    float floatValue = 0;
                    memcpy(&floatValue,(char*)data+offset,recordDescriptor[i].length);
                    offset += recordDescriptor[i].length;
                    cout <<"    "<< recordDescriptor[i].name << ": " << floatValue;
                }else { //  TypeVarChar
                    char* strValue = (char*) malloc(tempInt);
                    memcpy(strValue,(char*)data+offset,tempInt);
                    offset += tempInt;
                    cout <<"    "<<recordDescriptor[i].name << ": " ;//
                    printStr(tempInt, strValue, stream);
                }
            }
            else{
                cout <<"    "<< recordDescriptor[i].name << ": NULL";
            }
        }
        cout<<endl;
        return 0;
    }



    bool RBFM_ScanIterator::getIsRecordSatisfied(const RID rid, RecordBasedFileManager &rbfm,
                                                 FileHandle handle) const {
        if(this->conditionAttributeAttr.length == -1 || this->compOp == NO_OP){//no condition , no need to filter
            return true;
        }
        void *attrDataToFilter = malloc(this->conditionAttributeAttr.length + 1);
        rbfm.readAttribute(handle, this->recordDescriptor,
                           rid, this->conditionAttributeAttr.name, attrDataToFilter);
        char * nullFieldsIndicator = (char*)malloc(1);
        memcpy(nullFieldsIndicator, attrDataToFilter, 1);
        if(nullFieldsIndicator[0] | (1 << 7) == 1){
            return false; //null
        }

        int intFilterValue = 0, int_attrDataToFilter = 0;
        float floatFilterValue = 0, float_attrDataToFilter = 0;
        int comRes = 0;

//  compare with filterValue
        switch (this->conditionAttributeAttr.type) {
            case TypeInt: {
                memcpy(&int_attrDataToFilter, (char*)attrDataToFilter + 1, sizeof(int));
                memcpy(&intFilterValue, this->filterValue, sizeof(int));
//            cout<<"intFilterValue = "<<intFilterValue<<endl;
                comRes =  int_attrDataToFilter - intFilterValue;
                break;
            }
            case TypeReal: {
                memcpy(&float_attrDataToFilter, (char*)attrDataToFilter + 1, sizeof(float));
                memcpy(&floatFilterValue, this->filterValue, sizeof(float));
                if (float_attrDataToFilter > floatFilterValue && fabs(float_attrDataToFilter - floatFilterValue) > 1E-6) { comRes = 1; }
                else if (float_attrDataToFilter < floatFilterValue && fabs(float_attrDataToFilter - floatFilterValue) > 1E-6) { comRes = -1; }
                else { comRes = 0; }
                break;
            }
            case TypeVarChar: {
                int filterValue_varcharLen = 0, fieldLen = 0;
                memcpy(&filterValue_varcharLen, this->filterValue, sizeof(int));
                memcpy(&fieldLen, (char*)attrDataToFilter + 1, sizeof(int));
                string dataToFilter((char *)attrDataToFilter + 1 + sizeof(int), fieldLen);
                string filteVal((char *)this->filterValue + sizeof(int), filterValue_varcharLen);
                comRes = strcmp(dataToFilter.c_str(), filteVal.c_str());
                break;
            }
        }

        bool isRecordSatisfied = false;

        switch (compOp) {
            case EQ_OP: { //=
                if (comRes == 0) { isRecordSatisfied = true; }
                break;
            }
            case GT_OP: { //>
                if (comRes > 0) { isRecordSatisfied = true; }
                break;
            }
            case LT_OP: { //<
                if (comRes < 0) { isRecordSatisfied = true; }
                break;
            }
            case LE_OP: { //<=
                if (comRes <= 0) { isRecordSatisfied = true; }
                break;
            }
            case GE_OP: { //>=
                if (comRes >= 0) { isRecordSatisfied = true; }
                break;
            }
            case NE_OP: { // !=
                if (comRes != 0) { isRecordSatisfied = true; }
                break;
            }
            case NO_OP: { //no condition
                isRecordSatisfied = true;
                break;
            }
        }
        return isRecordSatisfied;
    }



    bool
    RBFM_ScanIterator::isDescriptorRequired(const vector<std::string> &attributeNames,
                                            const string &name) const {
        bool flag = false;
        for (int j = 0; j < attributeNames.size(); j++) {
            if (strcmp(attributeNames[j].c_str(), name.c_str()) == 0) {
                flag = true;
                break;
            }
        }
        return flag;
    }


    RC RBFM_ScanIterator::close() {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        rbfm.closeFile(this->iteratorHandle);
        vector <Attribute>().swap(selectedRecordDescriptor);
        vector <Attribute>().swap(recordDescriptor);
        return 0;
    }


} // namespace PeterDB
