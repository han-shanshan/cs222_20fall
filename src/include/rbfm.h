#ifndef _rbfm_h_
#define _rbfm_h_

#include <math.h>

#include "pfm.h"
using namespace std;

#define SLOT_TABLE_FIELD_LEN 4
namespace PeterDB {
    // Record ID
    typedef struct {
        unsigned pageNum;           // page number
        unsigned short slotNum;     // slot number in the page
    } RID;

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;

    typedef struct Attribute {
        std::string name;  // attribute fileName
        AttrType type;     // attribute type
        AttrLength length; // attribute length
    } Attribute;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;


    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();
    class RecordBasedFileManager;
    class RBFM_ScanIterator {
    public:
        RBFM_ScanIterator() = default;;

        ~RBFM_ScanIterator() = default;;
//        RecordBasedFileManager& rbfm();
//        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &rid, void *data);

        RC close();// { return -1; };
        //need to initialize
        FileHandle iteratorHandle;
        std::vector<Attribute> recordDescriptor;
        std::vector<Attribute> selectedRecordDescriptor;
        Attribute conditionAttributeAttr;
        CompOp compOp;
        char filterValue[PAGE_SIZE];
//        void *filterValue = malloc(PAGE_SIZE);
        std::vector<std::string> attributeNames;
        bool isIteratorNew; //用于判断是否需要从0， 0开始读
        RID lastRID;

        bool isDescriptorRequired(const std::vector<std::string> &attributeNames, const std::string &name) const;

//    bool getIsRecordSatisfied(const RID &rid, RecordBasedFileManager &rbfm, void *attrDataToFilter, FileHandle handle) const;

        bool getIsRecordSatisfied(const RID rid, FileHandle handle) const;

        int getTheCurrentData(RID rid, void *data);

//        void setIterator(RBFM_ScanIterator &iterator, RID &rid) const;
    };

    class RecordBasedFileManager {
    public:
        static RecordBasedFileManager &instance();                          // Access to the singleton instance
        PagedFileManager &pfm = PagedFileManager::instance();
        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field value is null or not.
        //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the value;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid);

        // Read a record identified by the given rid.
        RC
        readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-fileName: field1-value  field2-fileName: field2-value ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid);

        // Read an attribute given its fileName and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle,
                const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);
        int encodeRecordData_returnSlotLength(const std::vector<Attribute> &recordDescriptor, const void *data, void *encodedData);

        RC insertEncodedRecord(FileHandle &fileHandle, RID &rid, const void *encodedData, int slotLength);

        bool isFileExisting(const string &fileName);
        int getSlotTableLength(void *pageData);
        int decodeData(const vector <Attribute> &recordDescriptor, void *data, const void *encodedData);
        void printStr(int varcharLen, const char *strValue, ostream &out) const;


        RC printEncodedRecord(const vector<Attribute> &recordDescriptor, const void *data);

    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment


        void getOffsetAndLengthUsingSlotNum(const int slotNum, const void *pageData, int slotTableLen, int &offset,
                                       int &length) const;

        void getRealDirectedPageNumAndSlotNum(int offset, int length, RID &directedRid) const;


        void formDataPageAfterDelete(void *pageData, int slotTableLen, int offset, int length,
                                     const void *newPageData);

        void updateOffsetsInSlotTable(const void *pageData, int slotTableLen, int moveLength, int offset,
                                      bool isDelete) const;

        void updateSlotTable_SetOffsetAndLengthBySlotNum(void *pageData, int slotTableLen, int slotNum, int offset,
                                                         int length) const;

        int updateOffsetUsingSlotNumber(const void *pageData, int slotTableLen, int tempOffset, int slotNum) const;

        int updateLengthUsingSlotNumber(void *pageData, int slotTableLen, int length, int slotNum) const;

        int
        getNullIndicatorStr(const std::vector<Attribute> &recordDescriptor, const void *data,
                            unsigned char *&nullIndicatorStr) const;

        void
        insertRecordInDirectedPage(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                   const void *data, const RID &rid, char *newPageData);

        void directed_setSlotForOldPage(FileHandle &fileHandle, const RID &rid, char *newPageData, RID newDirectedRid);

        void
        computeTheDirectedOffsetAndLengthInTheOldPage(const RID &directedRid, int &newOffset, int &newLength) const;

        RC setLenToNegLenInDirectedPage(FileHandle &fileHandle, const RID &directedPageRid);

//        void formDataPageAfterUpdateOrDelete(void *newPageData, void *pageData,
//                const void *data, int slotTableLen, int offset, int length, int newSlotLength);

        bool isAttrFound(const std::string &filterAttributeName, int i_recordDescriptorCounter,
                         const std::vector<Attribute> &recordDescriptor) const;

//        void printStr(int varcharLen, const char *strValue) const;


        int getLastInteger(void *pageData, int index);

        int getFreeSpc(void *pageData);
        void updateLastInteger(void *newPageData, int val, int idx);
        void
        formDataPageAfterUpdate(char *newPageData, const char *pageData, const void *data,
                                int slotTableLen,
                                int offset, int length, int newSlotLength, int &freeSpc);

        void updateFreeSpc(void *pageData, int freeSpc);

        void updateSlotTableLen(void *pageData, int len);

        void addPageTail(void *pageData, int freeSpc, char *slotTable, int slotTableLen);

    };

} // namespace PeterDB

#endif // _rbfm_h_
