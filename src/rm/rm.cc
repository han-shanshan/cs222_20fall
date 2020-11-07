#include "src/include/rm.h"
#include "cmath"
#include <string.h>
#include <string>
#include<iostream>
#include <cstring>
#include <sstream>

using namespace std;
namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;
//todo:
    RelationManager::~RelationManager() = default;
//    RelationManager::~RelationManager() { delete _relation_manager; }

    RelationManager::RelationManager(const RelationManager &) = default;

//    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        if(rbfm.isFileExisting(TABLE_CATALOG_FILE)){rbfm.destroyFile(TABLE_CATALOG_FILE);} // for test cases: they do not delete system tables after runnning
        if(rbfm.isFileExisting(COLUMN_CATALOG_FILE)){rbfm.destroyFile(COLUMN_CATALOG_FILE);} // for test cases: they do not delete system tables after runnning
//        if(rbfm.isFileExisting(INDEX_CATALOG_FILE)){rbfm.destroyFile(INDEX_CATALOG_FILE);} // for test cases: they do not delete system tables after runnning

        //Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
        //Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
        if(createTable(TABLE_CATALOG_FILE, getTablesTableDescriptor()) == 0
           && createTable(COLUMN_CATALOG_FILE, getColumnsTableDescriptor()) == 0
//           && createSystemTable(INDEXES_TABLE_NAME, getIndexesTableDescriptor()) == 0
           ){return 0;}
        else
            return -1;
    }

    vector<Attribute> RelationManager::getColumnsTableDescriptor() {
        vector<Attribute> columnAttrs;
        columnAttrs.push_back(attribute("table-id", TypeInt, 4));
        columnAttrs.push_back(attribute("column-name", TypeVarChar, 50));
        columnAttrs.push_back(attribute("column-type", TypeInt, 4));
        columnAttrs.push_back(attribute("column-length", TypeInt, 4));
        columnAttrs.push_back(attribute("column-position", TypeInt, 4));
        return columnAttrs;
    }

    vector<Attribute> RelationManager::getTablesTableDescriptor() {
        vector<Attribute> tableAttrs;
        tableAttrs.push_back(attribute("table-id", TypeInt, 4));
        tableAttrs.push_back(attribute("table-name", TypeVarChar, 50));
        tableAttrs.push_back(attribute("file-name", TypeVarChar, 50));
        return tableAttrs;
    }

    vector<Attribute> RelationManager::getIndexesTableDescriptor() {
        vector<Attribute> indexAttrs;
        indexAttrs.push_back(attribute("table-id", TypeInt, 4));
        indexAttrs.push_back(attribute("table-name", TypeVarChar, 50));
        indexAttrs.push_back(attribute("column-pos", TypeInt, 4));
        indexAttrs.push_back(attribute("column-name", TypeVarChar, 50));
        return indexAttrs;
    }


    Attribute RelationManager::attribute(string name, AttrType type, int length){
        Attribute attr;
        attr.name = name;
        attr.type = type;
        attr.length = length;
        return attr;
    }


    RC RelationManager::deleteCatalog() {
        string tablesTableName = TABLE_CATALOG_FILE;
        string columnsTableName = COLUMN_CATALOG_FILE;
        FileHandle fileHandle;
        RC rc = rbfm.openFile(tablesTableName, fileHandle);
        if(rc != 0){ return -1; } //cout<<"fail to delete the record: the file has already been deleted."<<endl;

        RBFM_ScanIterator iterator;
        vector<std::string> attributeNames;
        attributeNames.push_back("table-name");
        RID rid;
        rid.pageNum = 0;
        rid.slotNum = 0;
        char filterValue[PAGE_SIZE];
        vector<Attribute> recordDescriptors = getTablesTableDescriptor();
        rbfm.scan(fileHandle, recordDescriptors, "",
                  NO_OP, filterValue, attributeNames, iterator);
        int fileNameLen = 0;
        int res = 0;

        char tempData[PAGE_SIZE];

        while (iterator.getNextRecord(rid, tempData) != RM_EOF) {
            memcpy(&fileNameLen, (char*)tempData + 1, sizeof(int));
            string fileName((char*)tempData + 1 + sizeof(int), fileNameLen);
//            string tablesTableName = TABLE_CATALOG_FILE; //((char *)this->filterValue + sizeof(int), filterValue_varcharLen);
//            string columnsTableName = COLUMN_CATALOG_FILE; //((char *)this->filterValue + sizeof(int), filterValue_varcharLen);
            if(strcmp(fileName.c_str(), tablesTableName.c_str()) != 0
               && strcmp(fileName.c_str(), columnsTableName.c_str()) != 0){
                rbfm.destroyFile(fileName);
            }
            memset(tempData, 0, PAGE_SIZE);
        }
        iterator.close();
        res = rbfm.destroyFile(TABLE_CATALOG_FILE);
        res = rbfm.destroyFile(COLUMN_CATALOG_FILE);
        res = rbfm.destroyFile(INDEX_CATALOG_FILE);
//        res = rbfm.destroyFile(SYSTEM_FILE_NAME);
        rbfm.closeFile(fileHandle);
        // Todo 删除所有index 文件
        return 0;
    }

//
//    RC RelationManager::createSystemTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
//        if(!isSystemTable(tableName)) {
//            return -1;
//        }
////        if(rbfm.isFileExisting(tableName)){
////            rbfm.destroyFile(tableName); // for test cases: they do not delete system tables after runnning
//////            return -1; //table already exists.
////        }
////        int table2NameLen = tableName.length();
////        void *systemRecord = malloc(sizeof(int) + tableNameLen);
////        memcpy((char*)systemRecord, &tableNameLen, sizeof(int));
////        memcpy((char*)systemRecord + sizeof(int), tableName.c_str(), tableNameLen);
////        FileHandle systemFH;
////        rbfm.openFile(systemFile, systemFH);
////        RID rid;
////        rbfm.insertEncodedRecord(systemFH, rid, systemRecord, sizeof(int) + tableNameLen);
////        rbfm.closeFile(systemFH);
//        return createTable(tableName, attrs);
//    }

    bool RelationManager::isSystemTable(const string &tableName){
        if(strcmp(tableName.c_str(), TABLE_CATALOG_FILE) == 0) {return true;}
        if(strcmp(tableName.c_str(), COLUMN_CATALOG_FILE) == 0) {return true;}
        if(strcmp(tableName.c_str(), INDEX_CATALOG_FILE) == 0) {return true;}
        return false;
    }


    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        string tableFileName = TABLE_CATALOG_FILE;
        if((!rbfm.isFileExisting(tableFileName)) &&
           !isSystemTable(tableName)) {return -1;} // catalog file does not exist
        if (rbfm.isFileExisting(tableName)) {return 1; }// cout << "Fail to create this table: the table is already exists. " << endl;}

        //if this table already exists, return 1;
        rbfm.createFile(tableName);
        RID tableRid;
        /* ******** insert into the catolog-table file ******** */
        vector<Attribute> tablesTableDescriptor = getTablesTableDescriptor();
        //to get max table id
        string columnName = "table-id";
        char value[PAGE_SIZE];
        int tableId = 1 + getMaxIntValueOfColumnName(columnName, "", NO_OP, value, TABLE_CATALOG_FILE, tablesTableDescriptor);
        vector<string> tableAttrValues;
        tableAttrValues.push_back(to_string(tableId));
        tableAttrValues.push_back(tableName);//tablename
        tableAttrValues.push_back(tableName);//table file
        char nullsIndicator[1];
        memset(nullsIndicator, 0, 1);//00000000
        char buffer[PAGE_SIZE];

        //prepare the table buffer. Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
        prepareDecodedRecord(nullsIndicator, tablesTableDescriptor, tableAttrValues, buffer);
//    cout<<"insert: ";
//        std::stringstream stream;
//        rbfm.printRecord(tablesTableDescriptor, buffer, stream);
        FileHandle tablCatalogFH;
        rbfm.openFile(tableFileName, tablCatalogFH);

//        rbfm.printRecord(tablesTableDescriptor, buffer, std::cout);
        rbfm.insertRecord(tablCatalogFH, tablesTableDescriptor, buffer, tableRid);
        rbfm.closeFile(tablCatalogFH);

        /* ******** insert into the catolog-column file ******** */
        //Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
        //(1, "table-id", TypeInt, 4 , 1)
        //    attr.name = "Salary";
        //    attr.type = TypeInt;
        //    attr.length = (AttrLength) 4;
        FileHandle colCatalogFH;
        string column_file_name = COLUMN_CATALOG_FILE;
        rbfm.openFile(column_file_name, colCatalogFH);
        vector<string> columnAttrValues;
        vector<Attribute> colTableDescriptor = getColumnsTableDescriptor();
        memset(nullsIndicator, 0, 1);//

        RID columnRid;
        int columnPosition = 1;
//    cout<<"insert into columns table --- "<<endl;
//    void * encodedTempData;
        for (int j = 0; j < attrs.size(); j++){
            columnAttrValues.push_back(to_string(tableId));
            columnAttrValues.push_back(attrs[j].name);
            columnAttrValues.push_back(to_string(convertTypeToInt(attrs[j].type))); //attrs[j].type
            columnAttrValues.push_back(to_string(attrs[j].length));
            columnAttrValues.push_back(to_string(columnPosition));
            columnPosition ++;
            //prepare the column buffer
            memset(buffer, 0, PAGE_SIZE);
            prepareDecodedRecord(nullsIndicator, colTableDescriptor, columnAttrValues, buffer);
//            rbfm.printRecord(colTableDescriptor, buffer, std::cout);
            rbfm.insertRecord(colCatalogFH, colTableDescriptor, buffer, columnRid);
//        this->printTuple(columnsTableDescriptor, buffer);
            vector <string>().swap(columnAttrValues);  //清除容器并最小化它的容量，
        }
//        cout<<endl;
        rbfm.closeFile(colCatalogFH);

        return 0;
    }


    int RelationManager::convertTypeToInt(AttrType type) const {
        int typeInt = 0;
        switch (type) {
            case TypeInt: {typeInt = 0; break; }
            case TypeReal: {typeInt = 1; break; }
            case TypeVarChar:{typeInt = 2; break;}
        }
        return typeInt;
    }

    /**
 * return >=0
 * @param rbfm
 * @param columnName
 * @param filterName
 * @param compOp
 * @param filterValue
 * @param fileName
 * @return
 */
    int RelationManager::getMaxIntValueOfColumnName(const string &columnName,
                                                    const string &filterName, const CompOp compOp, const void* filterValue,
                                                    const string &fileName, vector<Attribute> recordDescriptors) {
        int flagTableId= 0;
        RBFM_ScanIterator iterator;
        vector<string> attributeNames;
        attributeNames.push_back(columnName);
        RID rid;
        rid.pageNum = 0;
        rid.slotNum = 0;
        FileHandle fileHandle;
        rbfm.openFile(fileName, fileHandle);

        rbfm.scan(fileHandle, recordDescriptors, filterName,
                  compOp, filterValue, attributeNames, iterator);
        // null indicator + int
//        memset(iterator.tempData, 0, PAGE_SIZE);
        int tempTableId = 0;
        char tempData[PAGE_SIZE];

        while(iterator.getNextRecord(rid, tempData) != RM_EOF) {
            memcpy(&tempTableId, (char*)tempData + 1, sizeof(int));
//        cout<<tempTableId<<"---temptable id "<<endl;
            if(tempTableId >= flagTableId){
                flagTableId = tempTableId;
            }
            memset(tempData, 0, PAGE_SIZE);// null indicator + intlen + int
        }
        iterator.close();
        rbfm.closeFile(fileHandle);
        return flagTableId;
    }


//if this table already exists, return 0;
//    RC RelationManager::isTableAlreadyExisted(const string &tableName) const {
//        FileHandle tableFileHandle;
//        bool isTableFileExist = false;
//        if(rbfm.openFile(tableName, tableFileHandle) == 0){
//            isTableFileExist = true;
//        }
//        rbfm.closeFile(tableFileHandle);
//        return isTableFileExist;
//    }


/**
 * prepare the encoded record to insert into a table; attrValues type has been converted
 * @param columnDescriptor
 * @param attrValues
 * @param buffer encoded data
 * @return length of buffer
 *
 * for varchar, the attr values record only the real varchar, and does not include the varcharLen (not len + actual chars)
 */
    int RelationManager:: prepareEncodedRecord(vector<Attribute> columnDescriptor, vector<string> attrValues, void *buffer) {
        int offset = 0;
        int intLen = 4;
        int varcharLen = 0;
        for(int i = 0; i < columnDescriptor.size(); i++){
            if(columnDescriptor[i].type == TypeInt || columnDescriptor[i].type == TypeReal){
                memcpy((char*)buffer + offset, &intLen, sizeof(int));
                offset += sizeof(int);
                memcpy((char*)buffer + offset, attrValues[i].c_str(), sizeof(int));
                offset += sizeof(int);
            }else{
                memcpy(&varcharLen, attrValues[i].c_str(), sizeof(int));
                memcpy((char*)buffer + offset, &varcharLen, sizeof(int));
                offset += sizeof(int);
                memcpy((char*)buffer + offset, attrValues[i].c_str(), attrValues[i].length());
                offset += attrValues[i].length();
            }
        }
        return offset;
    }


    int RelationManager:: prepareDecodedRecord(char* nullFieldsIndicator,
                                               vector<Attribute> columnDescriptor, vector<string> attrValues, void *buffer) {
        int offset = 0;
        int fieldCount = columnDescriptor.size();
        // Null-indicators
        int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);
//        char nullFieldIndicator[nullFieldsIndicatorActualSize];
        memcpy((char *) buffer, nullFieldsIndicator, nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;
        int tempInt = 0, varcharLen = 0;
        float tempFloat = 0;

        for(int i = 0; i < columnDescriptor.size(); i++){
            if(!isFieldNull(nullFieldsIndicator, i)){ // this field is not null
                if(columnDescriptor[i].type == TypeInt ){
                    tempInt = stoi(attrValues[i]);
                    memcpy((char*)buffer + offset, &tempInt, columnDescriptor[i].length);
                    offset += columnDescriptor[i].length;
                }
                else if(columnDescriptor[i].type == TypeReal){
                    tempFloat = stof(attrValues[i]);
                    memcpy((char*)buffer + offset, &tempFloat, columnDescriptor[i].length);
                    offset += columnDescriptor[i].length;
                }
                else{ //varchar
                    varcharLen = attrValues[i].length();
                    memcpy((char*)buffer + offset, &varcharLen, sizeof(int));
                    offset += sizeof(int);
                    memcpy((char*)buffer + offset, attrValues[i].c_str(), varcharLen);
                    offset += varcharLen;
                }
            }
        }
    }

//1: null; 0: not null
    bool RelationManager::isFieldNull(char *nullFieldsIndicator, int i) const {
        return (nullFieldsIndicator[i / CHAR_BIT] & (1 << (7 - (i % CHAR_BIT))) == 0) == 1;
    }


    RC RelationManager::deleteTable(const std::string &tableName) {
        if(isSystemTable(tableName)) { return -1;} //cout<<"Fail to delete the system table"<<endl;
        if(!rbfm.isFileExisting(tableName)) {
            return -1; //table does not exist
        }
        RBFM_ScanIterator tableIterator, columnIterator;
        int tableId = getTableIdUsingTableName(tableName);
        if(tableId == -1) {return -1; } //cout << "Fail to delete the table." << endl;
        int rc = rbfm.destroyFile(tableName);
        if (rc != 0) return -1;
        RID tableRid, columnRid;
        tableRid.pageNum = 0;
        tableRid.slotNum = 0;
        columnRid.pageNum = 0;
        columnRid.slotNum = 0;
        vector<Attribute> recordDescriptor_column = getColumnsTableDescriptor();
        vector<Attribute> recordDescriptor_table = getTablesTableDescriptor();
        vector<string> attributeNames2;
        FileHandle fileHandle_table;
        string table_catalog_file = TABLE_CATALOG_FILE;
        rbfm.openFile(table_catalog_file, fileHandle_table);
        char filterValue[PAGE_SIZE];
        char tempData[PAGE_SIZE];
        memcpy(filterValue, &tableId, sizeof(int));
        ////////////////////////////////////delete records in Tables table
        rbfm.scan(fileHandle_table, recordDescriptor_table, "table-id",
                  EQ_OP, filterValue, attributeNames2, tableIterator);

        if (tableIterator.getNextRecord(tableRid, tempData) != RM_EOF) {
//        cout<<"table rid: "<<tableRid.pageNum<<"-"<<tableRid.slotNum<<endl;
            RC res = rbfm.deleteRecord(tableIterator.iteratorHandle, recordDescriptor_table, tableRid);
            if(res != 0) {return -1; }
        }
        tableIterator.close();
        rbfm.closeFile(fileHandle_table);
//
//        /////////////////////////////////delete records in Columns table
//        FileHandle fileHandle_column;
//        string column_catalog_file = COLUMN_CATALOG_FILE;
//        rbfm.openFile(column_catalog_file, fileHandle_column);
//        //filterValue is also tableId
//        rbfm.scan(fileHandle_column, recordDescriptor_column, "table-id",
//                  EQ_OP, filterValue, attributeNames2, columnIterator);
////        memset(columnIterator.tempData, 0, PAGE_SIZE);
//
//        while (columnIterator.getNextRecord(columnRid, tempData) != RM_EOF) {
////            cout<<"columnRid: "<<columnRid.pageNum<<"-"<<columnRid.slotNum<<endl;
//            RC res = rbfm.deleteRecord(columnIterator.iteratorHandle, recordDescriptor_column, columnRid);
//            if(res != 0) {return -1;}
//        }
//        columnIterator.close();
//        rbfm.closeFile(fileHandle_column);
////        vector<Attribute> attrs;
//        // todo:
////        this->getAttributes(tableName, attrs);
////        for(int i = 0; i < attrs.size(); i++) {
////            string indexFileName = getIndexFileName(tableName, attrs[i].name);
////            if(isTableAlreadyExisted(indexFileName)) {
////                rbfm.destroyFile(indexFileName);
////            }
////        }
        return 0;
    }

    int RelationManager::getTableIdUsingTableName(const string tableName) {
        FileHandle fileHandle_table;
        RBFM_ScanIterator tableIdIterator;
        int tableId;
        vector<Attribute> recordDescriptor_table = getTablesTableDescriptor();
        rbfm.openFile(TABLE_CATALOG_FILE, fileHandle_table);
        vector<string> attributeName_tableid;
        attributeName_tableid.push_back("table-id");
        //delete records in Tables table
        char filterValue[PAGE_SIZE]; // sizeof(int) + table name
        int tableNameLen = tableName.length();
        memcpy(filterValue, &tableNameLen, sizeof(int));
        memcpy((char*)filterValue + sizeof(int), tableName.c_str(), tableNameLen);
        rbfm.scan(fileHandle_table, recordDescriptor_table, "table-name",
                  EQ_OP, filterValue, attributeName_tableid, tableIdIterator);
        char tempData[PAGE_SIZE];
        RID tableIdRid;
        tableIdRid.pageNum = 0;
        tableIdRid.slotNum = 0;
//        if (tableIdIterator.getNextRecord(tableIdRid, tempData) != RM_EOF) {
//            rbfm.readAttribute(tableIdIterator.iteratorHandle, recordDescriptor_table, tableIdRid, "table-id", tempData);
//            memcpy(&tableId, (char*)tempData + 1, sizeof(int)); //
//        }
        tableIdIterator.close();
        RC res = rbfm.closeFile(fileHandle_table);
        if(res != 0) return -1;
        return tableId;
    }


    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        if(strcmp(tableName.c_str(), TABLE_CATALOG_FILE) == 0 ){
            attrs = getTablesTableDescriptor();
            return 0;
        }
        if(strcmp(tableName.c_str(), COLUMN_CATALOG_FILE) == 0 ){
            attrs =  getColumnsTableDescriptor();
            return 0;
        }

        FileHandle fileHandle;
        rbfm.openFile(COLUMN_CATALOG_FILE, fileHandle);
//    cout<<"table name = "<<tableName<<endl;
        char columnPositions4RecordDescriptors[PAGE_SIZE];
        int columnPositions4RecordDescriptors_offset = 0;
        int tableId = getTableIdUsingTableName(tableName);
//    cout<<"table id = "<<tableId<<endl;
//    Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)

        RBFM_ScanIterator iterator;
        vector<Attribute> columnRecordDescriptors = getColumnsTableDescriptor();
        vector<Attribute> columnRecordDescriptors_Output;
        vector<string> attributeNames;
        for(int i = 1; i < columnRecordDescriptors.size(); i++){
            attributeNames.push_back(columnRecordDescriptors[i].name);
            columnRecordDescriptors_Output.push_back(columnRecordDescriptors[i]);
        }

        RID rid2;
        rid2.pageNum = 0;
        rid2.slotNum = 0;
        ////////////////////////////////////
        char value[PAGE_SIZE];
        memcpy(value, &tableId, sizeof(int));
        rbfm.scan(fileHandle, columnRecordDescriptors, "table-id",
                  EQ_OP, &value, attributeNames, iterator);
//        memset(&value, 0, PAGE_SIZE);

//        memset(iterator.pageData, 0, PAGE_SIZE);
//    int tempTableId = 0;
        int varcharLen = 0;
        int columnType = 0;
        int columnLen = 0, columnPosition = 0;
        int offset = 0;
        AttrType type;
        char encodedFilteredData[PAGE_SIZE];
        char tempData[PAGE_SIZE];
        char recordData[PAGE_SIZE];

        while (iterator.getNextRecord(rid2, recordData) != RM_EOF) {
            offset = 0;
//            memset(iterator.tempData, 0, PAGE_SIZE);
            rbfm.encodeRecordData_returnSlotLength(columnRecordDescriptors_Output, recordData, tempData);
            memcpy(&varcharLen, tempData, sizeof(int));
            offset += sizeof(int);
            memcpy(encodedFilteredData, (char*)tempData + offset, varcharLen);
            offset += varcharLen;
            offset += sizeof(int);
            memcpy(&columnType, (char*)tempData + offset, sizeof(int));
            offset += sizeof(int);
            offset += sizeof(int);
            if(columnType == 0){type = TypeInt; }
            else if(columnType == 1){type = TypeReal; }
            else if(columnType == 2){type = TypeVarChar; }
            memcpy(&columnLen, (char*)tempData + offset, sizeof(int));
            offset += sizeof(int);
            offset += sizeof(int);
            memcpy(&columnPosition, (char*)tempData + offset, sizeof(int));
            attrs.push_back(attribute(string((char*)encodedFilteredData, varcharLen), type, columnLen));
            memcpy((char*)columnPositions4RecordDescriptors + columnPositions4RecordDescriptors_offset,
                   &columnPosition, sizeof(int));
            columnPositions4RecordDescriptors_offset += sizeof(int);
            memset(recordData, 0, PAGE_SIZE); // null indicator
            memset(tempData, 0, PAGE_SIZE);
        }
        iterator.close();
        rbfm.closeFile(fileHandle);
        //order  columnPositions4RecordDescriptors  void*        recordDescriptor
        orderRecordDescriptor(attrs, columnPositions4RecordDescriptors);
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        vector<Attribute> recordDescriptor;
        this->getAttributes(tableName, recordDescriptor);
        FileHandle fileHandle;
        int res = rbfm.openFile(tableName, fileHandle);
        if(res != 0){
//            cout<<"Fail to insert the tuple: the table "<<tableName<<" is not existed."<<endl;
            return -1;
        }
        res = rbfm.insertRecord(fileHandle, recordDescriptor, data, rid);
        if(res != 0){
            rbfm.closeFile(fileHandle);
            return -1;
        }
        res = rbfm.closeFile(fileHandle);
        if(res != 0){return -1; }
        return 0;
    }


    void RelationManager::orderRecordDescriptor(vector<Attribute> &recordDescriptor,
                                                const void *columnPositions4RecordDescriptors) const {
        int size = recordDescriptor.size();
        int columnPositions[size];
        int tempPostion = 0;
        int offset_columnPostion = 0;
        for(int j = 0; j < size; j++){
            memcpy(&tempPostion, (char*)columnPositions4RecordDescriptors + offset_columnPostion, sizeof(int));
            columnPositions[j] = tempPostion;
            offset_columnPostion += sizeof(int);
        }
        //-------order
        int tempPosition = 0;
        Attribute tempAttr;
        //bubble
        for(int i=0;i<size;i++) {
            for(int j=i+1;j<size;j++) {
                if(columnPositions[i] > columnPositions[j]) {
                    tempPosition = columnPositions[i];
                    columnPositions[i] = columnPositions[j];
                    columnPositions[j] = tempPosition;
                    swap(recordDescriptor[i], recordDescriptor[j]);
                }
            }
        }
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        vector<Attribute> recordDescriptor;
        this->getAttributes(tableName, recordDescriptor);
        FileHandle fileHandle;
        rbfm.openFile(tableName, fileHandle);
        int res = rbfm.deleteRecord(fileHandle, recordDescriptor, rid);
        rbfm.closeFile(fileHandle);
        return res;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        vector<Attribute> recordDescriptor;
        getAttributes(tableName, recordDescriptor);
        FileHandle fileHandle;
        rbfm.openFile(tableName, fileHandle);
        int res = rbfm.updateRecord(fileHandle, recordDescriptor,data, rid);
        rbfm.closeFile(fileHandle);
        return res;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        vector<Attribute> recordDescriptor;
        this->getAttributes(tableName, recordDescriptor);
        FileHandle fileHandle;
        if(rbfm.openFile(tableName, fileHandle) != 0){
//            cout<<"Fail to read the tuple: the table has been deleted"<<endl;
            return -1;
        }
        int res = rbfm.readRecord(fileHandle, recordDescriptor, rid, data);
        rbfm.closeFile(fileHandle);
        return res;
    }

    string
    RelationManager::getIndexFileName(const string &tableName, const string &attributeName) const { return tableName + "_" + attributeName + ".idx"; }



    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        rbfm.printRecord(attrs, data, out);
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        vector<Attribute> recordDescriptor;
        this->getAttributes(tableName, recordDescriptor);
        FileHandle fileHandle;
        rbfm.openFile(tableName, fileHandle);
        rbfm.readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        FileHandle fileHandle;
        if(!rbfm.isFileExisting(tableName)) {return -1;}

        if(strcmp(tableName.c_str(), TABLE_CATALOG_FILE) == 0 ){
            rbfm.openFile(TABLE_CATALOG_FILE, fileHandle);
        }
        else if(strcmp(tableName.c_str(), COLUMN_CATALOG_FILE) == 0 ){
            rbfm.openFile(COLUMN_CATALOG_FILE, fileHandle);
        }else rbfm.openFile(tableName, fileHandle);
//    RBFM_ScanIterator rbfmScanner;
        vector<Attribute> recordDescriptor;
        this->getAttributes(tableName, recordDescriptor);
        rbfm.scan(fileHandle, recordDescriptor, conditionAttribute, compOp,
                  value, attributeNames, rm_ScanIterator.rbfm_scanner);
//    rbfm.closeFile(fileHandle);
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return rbfm_scanner.getNextRecord(rid, data); }

    RC RM_ScanIterator::close() {
        this->rbfm_scanner.close();
        return 0;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

} // namespace PeterDB