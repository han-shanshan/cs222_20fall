#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    struct SplitInfo {
        bool isSplit;
        char promotedPKeyP[PAGE_SIZE];
    };

    class IX_ScanIterator;

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;
//        int rootNode;
        FileHandle fh;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    };

    class IndexManager {

    public:
        static IndexManager &instance();
        PagedFileManager &pfm = PagedFileManager::instance();
        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;
        bool isLeafNode(const void *page, int &freeSpc) const;
        int getFreeSpc(void *page) const ;
        int compareValue(void* key1, void *key2, AttrType type) const;
        int traverseTree(IXFileHandle &ixFileHandle, int node, Attribute attribute, int layer, std::ostream &out) const;
        void updateFreeSpc(void *pageData, int val) const;
        int getNextPageID(void *page) const;
        int getSubTreePageNum(const Attribute &attribute, const void* key, const void *page,
                              int lenOfAllRecords, bool isKeyNull) const;
        int getLenOfRecords(int freeSpc) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

//        int getRootNodeInHiddenPage(IXFileHandle &ixFileHandle) const;
        RC insertEntry_inner(IXFileHandle &ixFileHandle, const Attribute &attribute,
                                           const void *key, const RID &rid, int currentNode, SplitInfo &splitInfo);
        int formKeyRidPair_returnLength(const Attribute &attribute, const void *key, const RID &rid,
                                                      void *keyRidPair);


        int formLeafNodeData(void *oldPage, Attribute attribute, const void *keyRidPair,
                                           int keyRidPairLen, int lenOfTheRecords, void * newPage) const;

        int addNewRootPage_leaf(IXFileHandle &ixFileHandle, const void *keyRidPair, int keyRidPairLen) const;
        int getSplitOffset_leaf(const Attribute &attribute, const void *page,
                                              int freeSpcLen) const;

        int updateNextPagePointer(char *page, int nextPagePointer) const;
        RC splitLeafNode(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                                         int currentNode, SplitInfo &splitInfo, const void *keyRidPair,
                                         int keyRidPairLen, void *page, int freeSpcLen,
                                         int splitOffset);

        int getKeyLen(void *key, const Attribute &attribute) const;
        int formInterNodePageData(void *page, Attribute attribute, SplitInfo splitInfo,
                int pKeypPairLen, int lenOfTheRecords, void * newPage) const;
        int getSplitOffset_internal(const Attribute &attribute, const void *page, int freeSpcLen) const;

        int splitTheInternalNode(IXFileHandle &ixFileHandle, const Attribute &attribute, int currentNode,
               SplitInfo &splitInfo, void *page, int freeSpcLen, int pKeypLen, int splitOffset) const;
//        RC searchAndDeleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
//                                const void *key, const RID &rid, int currentNode);

        void printStr(int varcharLen, const char *strValue, ostream &out) const;

        int getFreeSpcIndicator(const void *page) const;

        void writeRoot(IXFileHandle &ixFileHandle, int root) const;

        int getRoot(IXFileHandle &ixFileHandle) const;
    };

    class IX_ScanIterator {
    public:
        Attribute attribute;
        char lowKey[PAGE_SIZE];
        char highKey[PAGE_SIZE];
        int lowKeyInclusive; // -1: low key not inclusive; 1: inclusive; 0: no low bound
        int highKeyInclusive;// -1: high key not inclusive; 1: inclusive; 0: no high bound
        IXFileHandle *ixFileHandle;
        int rootNode;
        int currentNode;
        int pageNum;
        int lastNode;
        int recordOffset;
//        int lastLength;
//        int lastOffset;
        char page[PAGE_SIZE];
        int freeSpc = -1;
        bool isIteratorNew;

        IndexManager &im = IndexManager::instance();

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
//        RC searchNextLeafEntry(void *returnedKey, RID &rid, void *key);
//        bool getPosIfLastKeyRidPairDeleted(void *key, RID rid, void *page);
        bool isHighKeySatisfied(void* currentKey);

        int getKeyLength(void *s, AttrType type) const;

        bool isLowKeySatisfied(void* currentKey);

        RC searchFirstEntry(void *returnedKey, RID &rid);


        RC searchNextLeafEntry(void *returnedKey, RID &rid);
    };


}// namespace PeterDB
#endif // _ix_h_
