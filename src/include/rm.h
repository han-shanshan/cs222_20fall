#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "src/include/rbfm.h"
#include "src/include/ix.h"

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

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator : public IX_ScanIterator {
    public:
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
//        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
//        RC close();                              // Terminate index scan
//        RM_ScanIterator rm_scanner;
//        IX_ScanIterator ix_scanner;
    };

    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        IndexManager &ix = IndexManager::instance();
        PagedFileManager &pfm = PagedFileManager::instance();

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

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

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

//        std::vector<Attribute> getIdxTableDescriptor();

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

        int getTableIdByName(const std::string &tableName);

//        RC createSystemTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        bool isSystemTable(const std::string &tableName);

        std::vector<Attribute> getIdxTableDescriptor();

        std::string
        getIndexFileName(const std::string &tableName, const std::string &attributeName) const;

        bool isTableEmpty(const std::string &tableName) const;

//        void getTableIdByName(const vector<Attribute> &recordDescriptor_table, const string &tableName,
//                                      int &tableId) const;
        int
        prepareDecodedRecord(char *nullFieldsIndicator, vector<Attribute> columnDescriptor, vector<string> attrValues,
                             void *buffer);

//        RC insertIntoColumnFile(const vector<Attribute> &attrs, int tableId);

//        int insertIntoTableFile_returnTableID(const string &tableName);
//
//        RC insertIntoColumnFile(int tableId);
        string getIdxFileName(const string &tableName, const string &attributeName) const;

        void constructVarcharFilterValue(const string &tableName, void *filterValue) const;

        Attribute getAttrWithName(const string &tableName, const string &attributeName);

        RC populateIndexFile(const string &tableName, const string &attributeName, const string &idxFileName);
    };

} // namespace PeterDB

#endif // _rm_h_