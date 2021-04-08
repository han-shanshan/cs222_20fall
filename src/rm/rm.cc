#include "src/include/rm.h"
#include "cmath"
#include <cstring>
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
        if(rbfm.isFileExisting(INDEX_CATALOG_FILE)){rbfm.destroyFile(INDEX_CATALOG_FILE);} // for test cases: they do not delete system tables after runnning
//        int res = 0;
        rbfm.createFile(TABLE_CATALOG_FILE);
        rbfm.createFile(COLUMN_CATALOG_FILE);
        rbfm.createFile(INDEX_CATALOG_FILE);
//        int tableTableId = insertIntoTableFile_returnTableID(TABLE_CATALOG_FILE);
//        insertIntoColumnFile(getTablesTableDescriptor(), tableTableId);
//        int columnTableId = insertIntoTableFile_returnTableID(TABLE_CATALOG_FILE);
//        insertIntoColumnFile(getColumnsTableDescriptor(), columnTableId);

        //Tables (table-id:int, table-fileName:varchar(50), file-fileName:varchar(50))
        //Columns(table-id:int, column-fileName:varchar(50), column-type:int, column-length:int, column-position:int)
        if(createTable(TABLE_CATALOG_FILE, getTablesTableDescriptor()) == 0
           && createTable(COLUMN_CATALOG_FILE, getColumnsTableDescriptor()) == 0
           && createTable(INDEX_CATALOG_FILE, getIdxTableDescriptor()) == 0
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

//    table-name; col-name; idx-file-name
    vector<Attribute> RelationManager::getIdxTableDescriptor() {
        vector<Attribute> idxAttrs;
//        idxAttrs.push_back(attribute("table-id", TypeInt, 4));
        idxAttrs.push_back(attribute("table-name", TypeVarChar, 50));
        idxAttrs.push_back(attribute("col-name", TypeVarChar, 50));
        idxAttrs.push_back(attribute("idx-file-name", TypeVarChar, 50));
        return idxAttrs;
    }


    Attribute RelationManager::attribute(const string &name, AttrType type, int length){
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
//        rid.pageNum = 0;
//        rid.slotNum = 0;
        vector<Attribute> recordDescriptors = getTablesTableDescriptor();
        rbfm.scan(fileHandle, recordDescriptors, "",
                  NO_OP, "", attributeNames, iterator);
        int fileNameLen = 0;
        int res = 0;
        char tempData[PAGE_SIZE];
//        iterator.isIteratorNew = true;
        while (iterator.getNextRecord(rid, tempData) != RM_EOF) {
            memcpy(&fileNameLen, (char*)tempData + 1, sizeof(int));
            string fileName((char*)tempData + 1 + sizeof(int), fileNameLen);
            if(!isSystemTable(fileName)) {
                if(rbfm.destroyFile(fileName)!= 0) return -1;
            }
        }
        iterator.close();
        if(rbfm.closeFile(fileHandle)!= 0) return -1;
        res = rbfm.destroyFile(TABLE_CATALOG_FILE);
        res = rbfm.destroyFile(COLUMN_CATALOG_FILE);
        res = rbfm.destroyFile(INDEX_CATALOG_FILE);
//        res = rbfm.destroyFile(SYSTEM_FILE_NAME);

        // Todo 删除所有index 文件
        return 0;
    }


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
        if (rbfm.isFileExisting(tableName)&& !isSystemTable(tableName)) {return 1; }// cout << "Fail to create this table: the table is already exists. " << endl;}

        //if this table already exists, return 1;
        if(!isSystemTable(tableName)) {
            rbfm.createFile(tableName);
        }
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

        //prepare the table buffer. Tables (table-id:int, table-fileName:varchar(50), file-fileName:varchar(50))
        prepareDecodedRecord(nullsIndicator, tablesTableDescriptor, tableAttrValues, buffer);
        FileHandle tablCatalogFH;
        rbfm.openFile(tableFileName, tablCatalogFH);

        rbfm.insertRecord(tablCatalogFH, tablesTableDescriptor, buffer, tableRid);
        rbfm.closeFile(tablCatalogFH);

        /* ******** insert into the catolog-column file ******** */
        //Columns(table-id:int, column-fileName:varchar(50), column-type:int, column-length:int, column-position:int)
        //(1, "table-id", TypeInt, 4 , 1)
        //    attr.fileName = "Salary";
        //    attr.type = TypeInt;
        //    attr.length = (AttrLength) 4;
        FileHandle colCatalogFH;
        string column_file_name = COLUMN_CATALOG_FILE;
        rbfm.openFile(column_file_name, colCatalogFH);
        vector<string> columnAttrValues;
        vector<Attribute> colTableDescriptor = getColumnsTableDescriptor();

        RID columnRid;
        int columnPosition = 1;
        for (int j = 0; j < attrs.size(); j++){
            columnAttrValues.push_back(to_string(tableId));
            columnAttrValues.push_back(attrs[j].name);
            columnAttrValues.push_back(to_string(convertTypeToInt(attrs[j].type))); //attrs[j].type
            columnAttrValues.push_back(to_string(attrs[j].length));
            columnAttrValues.push_back(to_string(columnPosition));
            columnPosition ++;
            //prepare the column buffer
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
//        rid.pageNum = 0;
//        rid.slotNum = 0;
        FileHandle fileHandle;
        rbfm.openFile(fileName, fileHandle);

        rbfm.scan(fileHandle, recordDescriptors, filterName,
                  compOp, filterValue, attributeNames, iterator);
        // null indicator + int
        int tempTableId = 0;
        char tempData[PAGE_SIZE];
        while(iterator.getNextRecord(rid, tempData) != RM_EOF) {
            memcpy(&tempTableId, (char*)tempData + 1, sizeof(int));
//        cout<<tempTableId<<"---temptable id "<<endl;
            if(tempTableId >= flagTableId){
                flagTableId = tempTableId;
            }
        }
        iterator.close();
        rbfm.closeFile(fileHandle);
        return flagTableId;
    }


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
    bool RelationManager::isFieldNull(const char *nullFieldsIndicator, int i) const {
        return (nullFieldsIndicator[i / CHAR_BIT] & (1 << (7 - (i % CHAR_BIT))) == 0) == 1;
    }


    RC RelationManager::deleteTable(const std::string &tableName) {
        if(isSystemTable(tableName)) { return -1;} //cout<<"Fail to delete the system table"<<endl;
        if(!rbfm.isFileExisting(tableName)) {
            return -1; //table does not exist
        }
        RBFM_ScanIterator tableIterator, columnIterator, idxIterator;
        int tableId = getTableIdByName(tableName);
        if(tableId == -1) {return -1; } //cout << "Fail to delete the table." << endl;
        int rc = rbfm.destroyFile(tableName);
        if (rc != 0) return -1;
        RID rid;
        vector<Attribute> colDescriptor = getColumnsTableDescriptor();
        vector<Attribute> tblDescriptor = getTablesTableDescriptor();
        vector<Attribute> idxDescriptor = getIdxTableDescriptor();
        vector<string> attrNames;
        FileHandle fileHandle_table;
        string table_catalog_file = TABLE_CATALOG_FILE;
        rbfm.openFile(table_catalog_file, fileHandle_table);
        char filterValue[PAGE_SIZE];
        char tempData[PAGE_SIZE];
        memcpy((char*)filterValue, &tableId, sizeof(int));
        //////////////////////////////////delete records in Tables table
        rbfm.scan(fileHandle_table, tblDescriptor, "table-id",
                  EQ_OP, filterValue, attrNames, tableIterator);
        if (tableIterator.getNextRecord(rid, tempData) != RM_EOF) {
//        cout<<"table rid: "<<tableRid.pageNum<<"-"<<tableRid.slotNum<<endl;
            RC res = rbfm.deleteRecord(tableIterator.iteratorHandle, tblDescriptor, rid);
            if(res != 0) {return -1; }
        }
        tableIterator.close();
        rbfm.closeFile(fileHandle_table);

        /////////////////////////////////delete records in Columns table
        FileHandle fileHandle_column;
        string column_catalog_file = COLUMN_CATALOG_FILE;
        rbfm.openFile(column_catalog_file, fileHandle_column);
        //filterValue is also tableId
        rbfm.scan(fileHandle_column, colDescriptor, "table-id",
                  EQ_OP, filterValue, attrNames, columnIterator);
        while (columnIterator.getNextRecord(rid, tempData) != RM_EOF) {
//            rbfm.printRecord(recordDescriptor_column, tempData, std::cout);
//            cout<<"columnRid: "<<columnRid.pageNum<<"-"<<columnRid.slotNum<<endl;
            RC res = rbfm.deleteRecord(columnIterator.iteratorHandle, colDescriptor, rid);
            if(res != 0) {return -1;}
        }
        columnIterator.close();
        rbfm.closeFile(fileHandle_column);

        ////////////delete records in Idx table
        FileHandle idxFH;
        string idx_file = INDEX_CATALOG_FILE;
        if(rbfm.openFile(idx_file, idxFH) != 0){return -1;}

//        memcpy((char*)filterValue, &tableId, sizeof(int));
        constructVarcharFilterValue(tableName, filterValue);
        attrNames.push_back("col-name");
        rbfm.scan(idxFH, idxDescriptor, "table-name",
                  EQ_OP, filterValue, attrNames, idxIterator);
        vector<Attribute> descriptor;
        Attribute a;
        a.type = TypeVarChar;
        a.length = 50;
        a.name = "col-name";
        descriptor.push_back(a);
        while (idxIterator.getNextRecord(rid, tempData) != RM_EOF) {
            if(rbfm.deleteRecord(idxIterator.iteratorHandle, idxDescriptor, rid) != 0) {return -1; }
            int res = rbfm.destroyFile(tableName + "_" + string(tempData + 5) + ".idx") ;
        }
        idxIterator.close();
        rbfm.closeFile(idxFH);
        return 0;
    }

    int RelationManager::getTableIdByName(const string &tableName) {
        FileHandle fileHandle_table;
        RBFM_ScanIterator tableIdIterator;
        int tableId = -1;
        vector<Attribute> recordDescriptor_table = getTablesTableDescriptor();
        rbfm.openFile(TABLE_CATALOG_FILE, fileHandle_table);
        vector<string> attributeName_tableid;
        attributeName_tableid.push_back("table-id");
        //delete records in Tables table
        char filterValue[PAGE_SIZE]; // sizeof(int) + table fileName
        int tableNameLen = tableName.length();
        memcpy(filterValue, &tableNameLen, sizeof(int));
        memcpy((char*)filterValue + sizeof(int), tableName.c_str(), tableNameLen);
        rbfm.scan(fileHandle_table, recordDescriptor_table, "table-name",
                  EQ_OP, filterValue, attributeName_tableid, tableIdIterator);

        char tempData[PAGE_SIZE];
        RID tableIdRid;
        if (tableIdIterator.getNextRecord(tableIdRid, tempData) != RM_EOF) {
            memcpy(&tableId, (char*)tempData + 1, sizeof(int)); //
        }
        tableIdIterator.close();
        RC res = rbfm.closeFile(fileHandle_table);
        if(res != 0) {return -1;}
        return tableId;
    }


    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        attrs.clear();
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
//    cout<<"table fileName = "<<tableName<<endl;
        char columnPositions4RecordDescriptors[PAGE_SIZE];
        int columnPositions4RecordDescriptors_offset = 0;
        int tableId = getTableIdByName(tableName);
//    cout<<"table id = "<<tableId<<endl;
//    Columns(table-id:int, column-fileName:varchar(50), column-type:int, column-length:int, column-position:int)

        RBFM_ScanIterator iterator;
        vector<Attribute> columnRecordDescriptors = getColumnsTableDescriptor();
        vector<Attribute> columnRecordDescriptors_Output;
        vector<string> attributeNames;
        for(int i = 1; i < columnRecordDescriptors.size(); i++){
            attributeNames.push_back(columnRecordDescriptors[i].name);
            columnRecordDescriptors_Output.push_back(columnRecordDescriptors[i]);
        }

        RID rid2;
//        rid2.pageNum = 0;
//        rid2.slotNum = 0;
        ////////////////////////////////////
        char value[PAGE_SIZE];
        memcpy(value, &tableId, sizeof(int));
        rbfm.scan(fileHandle, columnRecordDescriptors, "table-id",
                  EQ_OP, &value, attributeNames, iterator);
        int varcharLen = 0;
        int columnType = 0;
        int columnLen = 0, columnPosition = 0;
        int offset = 0;
        AttrType type = TypeInt;
        char encodedFilteredData[PAGE_SIZE];
        char tempData[PAGE_SIZE];
        char recordData[PAGE_SIZE];
        while (iterator.getNextRecord(rid2, recordData) != RM_EOF) {
            offset = 0;
//            rbfm.printRecord(columnRecordDescriptors, recordData, std::cout);
            int slotLne = rbfm.encodeRecordData_returnSlotLength(columnRecordDescriptors_Output, recordData, tempData);
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
        vector<Attribute> recordDescriptor;
        this->getAttributes(tableName, recordDescriptor);
        rbfm.scan(fileHandle, recordDescriptor, conditionAttribute, compOp,
                  value, attributeNames, rm_ScanIterator.rbfm_scanner);
        return 0;
    }

    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        vector<string> idxAttrValues;
        vector<Attribute> idxTableDescriptor = getIdxTableDescriptor();
        string idxFileName = getIdxFileName(tableName, attributeName);
        char buffer[PAGE_SIZE];
        char nullsIndicator[1];
        memset(nullsIndicator, 0, 1);//00000000
        RID idxRid;
        idxAttrValues.push_back(tableName);
        idxAttrValues.push_back(attributeName); //attrs[j].type
        idxAttrValues.push_back(idxFileName);
        prepareDecodedRecord(nullsIndicator, idxTableDescriptor, idxAttrValues, buffer);
        FileHandle fh;
        if(rbfm.openFile(INDEX_CATALOG_FILE, fh)!=0){return  -1;}
        if(rbfm.insertRecord(fh, idxTableDescriptor, buffer, idxRid)!= 0) {return -1;}
        rbfm.closeFile(fh);


        if(ix.createFile(idxFileName)!=0){return -1; }
//        FileHandle tab2lefh;
//        if(rbfm.openFile(tableName, tablefh) != 0) {return 0;}
        return populateIndexFile(tableName, attributeName, idxFileName);

    }

    RC RelationManager::populateIndexFile(const string &tableName, const string &attributeName,
                                          const string &idxFileName) {
        if(!rbfm.isFileExisting(tableName)) {return 0;}
        RM_ScanIterator rm_scanner;
        vector<string> attrNames;
        attrNames.push_back(attributeName);
        vector<Attribute> descriptor;
        getAttributes(tableName, descriptor);
        Attribute attr;
        for(Attribute a: descriptor) {
            if(strcmp(a.name.c_str(), attributeName.c_str()) == 0) {
                attr = a;
                break;
            }
        }
        scan(tableName, "", NO_OP, "", attrNames, rm_scanner);

        char tempData[PAGE_SIZE], tempData2[PAGE_SIZE];
        RID tempRID, tempRID2;
        IXFileHandle idxFH;
//        int varlen = 0;

        vector<Attribute> tempDescriptor;
        tempDescriptor.push_back(attr);
        if(ix.openFile(idxFileName, idxFH) != 0 ) {return -1;}
        while(rm_scanner.getNextTuple(tempRID, tempData) != RM_EOF) {
//            rbfm.printRecord(tempDescriptor, tempData, std::cout);
            if(ix.insertEntry(idxFH, attr, (char*)tempData + 1, tempRID) != 0) {return -2;}
        }
//        ix.printBTree(idxFH, attr, std::cout);
        ix.closeFile(idxFH);
        rm_scanner.close();

        return 0;
    }

    string
    RelationManager::getIdxFileName(const string &tableName, const string &attributeName) const { return tableName + "_" + attributeName + ".idx"; }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
        RBFM_ScanIterator idxIterator;
        if(rbfm.destroyFile(getIdxFileName(tableName, attributeName)) != 0){return -1;}
        RID idxRid;
        vector<Attribute> recordDescriptor_idx = getIdxTableDescriptor();
        vector<string> attributeNames;
        attributeNames.push_back("col-name");
        FileHandle idxFH;
        string idx_catalog_file = INDEX_CATALOG_FILE;
        if(rbfm.openFile(idx_catalog_file, idxFH) != 0){return -1; }
        char tempData[PAGE_SIZE];
        char filterValue[PAGE_SIZE]; // sizeof(int) + table fileName
        constructVarcharFilterValue(tableName, filterValue);
        //////////////////////////////////delete records in Idx table
//        table-name; col-name; idx-file-name
        rbfm.scan(idxFH, recordDescriptor_idx, "table-name",
                  EQ_OP, filterValue, attributeNames, idxIterator);
        if (idxIterator.getNextRecord(idxRid, tempData) != RM_EOF) {
            if(strcmp(attributeName.c_str(), (char*)tempData + INT_FIELD_LEN) == 0){
                if(rbfm.deleteRecord(idxIterator.iteratorHandle, recordDescriptor_idx, idxRid) != 0) {return -1;}
            }
        }
        idxIterator.close();
        rbfm.closeFile(idxFH);
        return 0;
    }

    void RelationManager::constructVarcharFilterValue(const string &tableName, void *filterValue) const {
        int tableNameLen = tableName.length();
        memcpy(filterValue, &tableNameLen, INT_FIELD_LEN);
        memcpy((char*)filterValue + INT_FIELD_LEN, tableName.c_str(), tableNameLen);
    }

    Attribute getAttribute(const string &tableName, const string &attributeName);

// indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator) {
        IXFileHandle ixFH;
        if(ix.openFile(getIdxFileName(tableName, attributeName), ixFH) != 0){return -1;}
        if(ixFH.fh.getNumberOfPages() <= 0) {
            populateIndexFile(tableName, attributeName, getIdxFileName(tableName, attributeName));
        }
        Attribute attribute = getAttrWithName(tableName, attributeName);
        ix.scan(ixFH, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator);
//        rm_IndexScanIterator.ix_scanner = &rm_IndexScanIterator
        return 0;
    }

    Attribute RelationManager::getAttrWithName(const string &tableName, const string &attributeName) {
        vector<Attribute> descriptor;
        this->getAttributes(tableName, descriptor);
        Attribute attribute;
        for(Attribute a: descriptor) {
            if(strcmp(a.name.c_str(), attributeName.c_str()) == 0) {
                attribute = a;
                break;
            }
        }
        return attribute;
    }

    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

//    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
//        return getNextEntry(rid, key);
//    }
//    RC RM_IndexScanIterator::close(){
//        return close();
//    }


    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return rbfm_scanner.getNextRecord(rid, data);
    }


    RC RM_ScanIterator::close() {
        return rbfm_scanner.close();
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