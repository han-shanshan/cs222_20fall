#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "src/include/rbfm.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator
    static const char *const TABLE_CATALOG_FILE = "Tables";
    static const char *const COLUMN_CATALOG_FILE = "Columns";
    static const char *const INDEX_CATALOG_FILE = "Indexes";
//    static const char *const SYSTEM_FILE_NAME = "Systems";

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RM_ScanIterator();

        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();

        RBFM_ScanIterator rbfm_scanner;
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    };

    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    private:
        static RelationManager *_relation_manager;

        Attribute attribute(const std::string &name, AttrType type, int length);

//        RC isTableAlreadyExisted(const std::string &tableName) const;

        std::vector<Attribute> getTablesTableDescriptor();

        std::vector<Attribute> getColumnsTableDescriptor();

/*
    int getMaxIntValueInTheFile(const std::string &fileName, std::vector<Attribute> &recordDescriptor,
                                const std::string &attributeName, RecordBasedFileManager &rbfm);

    int getMaxColumnPositionInTheFile(const std::string &fileName, std::vector<Attribute> &recordDescriptor,
                                      const std::string &attributeName, RecordBasedFileManager &rbfm,
                                      const std::string &filterAttributeName, int filterValue);

    int prepareColumnRecord(std::string &nullFieldsIndicator, std::vector<Attribute> columnDescriptor, Attribute attr,
                            int tableId, int columnPostion, void *buffer);
                            */

//        int prepareDecodedRecord(char *nullFieldsIndicator, std::vector<Attribute> columnDescriptor,
//                                 std::vector<std::string> attrValues, void *buffer);

        bool isFieldNull(const char *nullFieldsIndicator, int i) const;

//        void getTableIdUsingIterator(const std::vector<Attribute> &recordDescriptor_table,
//                                     RBFM_ScanIterator &iterator4Table, int &tableId) const;

        void orderRecordDescriptor(std::vector<Attribute> &recordDescriptor,
                                   const void *columnPositions4RecordDescriptors) const;

//    std::vector<Attribute>
//    getRecordDescriptor(const std::string &tableName);

//    void convertTypeToInt(int j, const std::vector<Attribute> &attrs, int &type) const;
        int convertTypeToInt(AttrType type) const;

        int prepareEncodedRecord(std::vector<Attribute> columnDescriptor, std::vector<std::string> attrValues, void *buffer);

        int
        getMaxIntValueOfColumnName(const std::string &columnName,
                                   const std::string &filterName,
                                   const CompOp compOp, const void *filterValue, const std::string &fileName,
                                   std::vector<Attribute> recordDescriptors);

        int getTableIdUsingTableName(const std::string &tableName);

//        RC createSystemTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        bool isSystemTable(const std::string &tableName);

        std::vector<Attribute> getIndexesTableDescriptor();

        std::string
        getIndexFileName(const std::string &tableName, const std::string &attributeName) const;

        bool isTableEmpty(const std::string &tableName) const;

//        void getTableIdUsingTableName(const vector<Attribute> &recordDescriptor_table, const string &tableName,
//                                      int &tableId) const;
        int
        prepareDecodedRecord(char *nullFieldsIndicator, vector<Attribute> columnDescriptor, vector<string> attrValues,
                             void *buffer);

//        RC insertIntoColumnFile(const vector<Attribute> &attrs, int tableId);

//        int insertIntoTableFile_returnTableID(const string &tableName);
//
//        RC insertIntoColumnFile(int tableId);
    };

} // namespace PeterDB

#endif // _rm_h_