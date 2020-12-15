#include "src/include/ix.h"
#include <sstream>
#include <iostream>
namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        int res = pfm.createFile(fileName);
        if(res != 0) {return -1;}
        FileHandle fileHandle;
        res = pfm.openFile(fileName, fileHandle);
        if(res != 0) {return -1; }
        return pfm.closeFile(fileHandle);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return pfm.destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        return pfm.openFile(fileName, ixFileHandle.fh);
    }


    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        ixFileHandle.fh.readPageCounter = 0;
        ixFileHandle.fh.writePageCounter = 0;
        ixFileHandle.fh.appendPageCounter = 0;
        ixFileHandle.ixReadPageCounter = 0;
        ixFileHandle.ixWritePageCounter = 0;
        ixFileHandle.ixAppendPageCounter = 0;
        return pfm.closeFile(ixFileHandle.fh);
    }
//todo use iterator
//    RC
//    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
////        int currentNode = getRoot(ixFileHandle);
//        SplitInfo splitInfo;
//        splitInfo.isSplit = false;
//
//
//        int offset = 0, pageNum = 0;
////        find pos for the inserted entry
//        IX_ScanIterator ix_ScanIterator;
//        scan(ixFileHandle, attribute, NULL, key, true, true, ix_ScanIterator);
////        RC IndexManager::scan(IXFileHandle &ixFileHandle, const Attribute &attribute,
////                              const void *lowKey, const void *highKey,
////                              bool lowKeyInclusive, bool highKeyInclusive,
////                              IX_ScanIterator &ix_ScanIterator)
//        RID temprid;
//        while (1) {
//            if(ix_ScanIterator.getNextEntry(temprid, &key) != 0){break;}
//        }
//        int freeSpc_leaf = ix_ScanIterator.freeSpc;
//        char page[PAGE_SIZE];
//        if(ixFileHandle.fh.getNumberOfPages() == 0){
////            todo:
//        }
//        if(ixFileHandle.fh.readPage(ix_ScanIterator.currentNode, page)!=0) {return -1;}
//        if(freeSpc_leaf > getKeyLen((char*)key, attribute)){
////            todo: form leaf page
//        }else
//
//
//
//        int res = insertEntry_inner(ixFileHandle, attribute, key, rid, currentNode, splitInfo);
//        if(splitInfo.isSplit) { //add new root
//            cout<<"aaaaaaaa"<<endl;
//            char newRootPage[PAGE_SIZE];
//            int promotedKeyLen = sizeof(int);
//            if(attribute.type == TypeVarChar) {
//                memcpy(&promotedKeyLen, (char*)splitInfo.promotedPKeyP + sizeof(int), sizeof(int));
//                promotedKeyLen += sizeof(int);
//            }
////        printStr(12, (char*)splitInfo.promotedPKeyP);
//            memcpy(newRootPage, splitInfo.promotedPKeyP, promotedKeyLen + 2 * sizeof(int));
//
//            int freeSpc = PAGE_SIZE - INT_FIELD_LEN - promotedKeyLen - 2 * INT_FIELD_LEN;
//            updateFreeSpc(newRootPage, freeSpc);
//            int newRootId = ixFileHandle.fh.getNumberOfPages();
//            ixFileHandle.fh.root = newRootId;
//            writeRoot(ixFileHandle, newRootId);
//            res = ixFileHandle.fh.appendPage(newRootPage);
//            if(res != 0) {return -1; }
//            cout<<"split rid: "<<rid.pageNum<<", "<<rid.slotNum<<endl;
//        }
//
//        return res;
//    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        int currentNode = getRoot(ixFileHandle);
        SplitInfo splitInfo;
        splitInfo.isSplit = false;

        int res = insertEntry_inner(ixFileHandle, attribute, key, rid, currentNode, splitInfo);
        if(splitInfo.isSplit) { //add new root
//            cout<<"aaaaaaaa"<<endl;
            char newRootPage[PAGE_SIZE];
            int promotedKeyLen = sizeof(int);
            if(attribute.type == TypeVarChar) {
                memcpy(&promotedKeyLen, (char*)splitInfo.promotedPKeyP + sizeof(int), sizeof(int));
                promotedKeyLen += sizeof(int);
            }
//        printStr(12, (char*)splitInfo.promotedPKeyP);
            memcpy(newRootPage, splitInfo.promotedPKeyP, promotedKeyLen + 2 * sizeof(int));

            int freeSpc = PAGE_SIZE - INT_FIELD_LEN - promotedKeyLen - 2 * INT_FIELD_LEN;
            updateFreeSpc(newRootPage, freeSpc);
            int newRootId = ixFileHandle.fh.getNumberOfPages();
            ixFileHandle.fh.root = newRootId;
            writeRoot(ixFileHandle, newRootId);
            res = ixFileHandle.fh.appendPage(newRootPage);
            if(res != 0) {return -1; }
            cout<<"split rid: "<<rid.pageNum<<", "<<rid.slotNum<<endl;
        }
//        IXFileHandle f;
//        pfm.openFile(ixFileHandle.fh.fileName, f.fh);
//        char data[PAGE_SIZE];
//        cout<<endl<<"freeSpc: ";
//        for(int j = 0; j < f.fh.getNumberOfPages(); j++) {
//            f.fh.readPage(j, data);
//            int freespc1 = this->getFreeSpcIndicator(data);
//            int freespc2 = this->getFreeSpc(data);
//            cout<<" p"<<j<<": "<<freespc1<<"-"<<freespc2;
//        }
//        pfm.closeFile(f.fh);

        return res;
    }
//todo: write root node
    void IndexManager::writeRoot(IXFileHandle &ixFileHandle, int root) const {
        ixFileHandle.ixWritePageCounter++;
        ixFileHandle.fh.writePageCounter++;
    }

    int IndexManager::addNewRootPage_leaf(IXFileHandle &ixFileHandle, const void *keyRidPair,
                                          int keyRidPairLen) const {
        char rootPage[PAGE_SIZE];
        int nextPageId = -1;
//        if(markAsLeaf(ixFileHandle, newRid)!=0) {return -2;} //fail to mark as leaf
        int freeSpc_rootPage = PAGE_SIZE - keyRidPairLen + pow(2, 14) - 2 * INT_FIELD_LEN; //marked as leaf node
        memcpy(rootPage, keyRidPair, keyRidPairLen);
        memcpy((char*)rootPage + PAGE_SIZE -INT_FIELD_LEN, &freeSpc_rootPage, INT_FIELD_LEN);
        memcpy((char*)rootPage + PAGE_SIZE - 2*INT_FIELD_LEN, &nextPageId, INT_FIELD_LEN);
        return ixFileHandle.fh.appendPage(rootPage);
    }

    RC IndexManager::insertEntry_inner(IXFileHandle &ixFileHandle, const Attribute &attribute,
                                       const void *key, const RID &rid, int currentNode, SplitInfo &splitInfo) {
        char keyRidPair[PAGE_SIZE];
        int keyRidPairLen = formKeyRidPair_returnLength(attribute, key, rid, keyRidPair);

        char page[PAGE_SIZE];
        int numOfPage = ixFileHandle.fh.getNumberOfPages();
        char newPage[PAGE_SIZE];
        int res;
        if((numOfPage < currentNode) || (numOfPage == currentNode && numOfPage != 0)) { return -1; }
        if(numOfPage == 0 && currentNode == 0) { //add root
            return addNewRootPage_leaf(ixFileHandle, keyRidPair, keyRidPairLen);
        }

        if(ixFileHandle.fh.readPage(currentNode, page) != 0) {return -1;}
//        cout<<currentNode<<", "<<getFreeSpc(page)<<"-----"<<endl;
        int freeSpc = -1;
        if(isLeafNode(page, freeSpc)) {
            if(freeSpc >= keyRidPairLen) { //add into the current page
                //todo: remove one page param
                res = formLeafNodeData(page, attribute, keyRidPair, keyRidPairLen, PAGE_SIZE - freeSpc - 2 * INT_FIELD_LEN, newPage);
                if(res != 0) {return -1; }
                res = ixFileHandle.fh.writePage(currentNode, newPage);
                if(res != 0) {return -1; }
                splitInfo.isSplit = false;
            }else { //full, split into 2
                int splitOffset = getSplitOffset_leaf(attribute, page, freeSpc);
                /**
                 * Todo 改分割方式
                 */
                splitInfo.isSplit = true;
                if(splitLeafNode(ixFileHandle, attribute, key, currentNode, splitInfo,
                              keyRidPair, keyRidPairLen, page, freeSpc, splitOffset)!=0) {return -1;}
            }
        }
        else { // inter node
            if(splitInfo.isSplit == false) { //search
//                cout<<endl<<endl<<"bbbbbbbbbb"<<endl;
                int subTreePageNum = getSubTreePageNum(attribute, key, page,
                        PAGE_SIZE - freeSpc - INT_FIELD_LEN, false);
                res = insertEntry_inner(ixFileHandle, attribute, key, rid, subTreePageNum, splitInfo);
                if(res != 0) {return -1;}
                if(splitInfo.isSplit) {
                    res = insertEntry_inner(ixFileHandle, attribute, key, rid, currentNode, splitInfo);
                    if(res != 0) {return -1;}
                }
            }else  {//INSERT
                int pKeypLen = 3 * sizeof(int);
                if (attribute.type == TypeVarChar) {
                    int varLen = -1;
                    memcpy(&varLen, (char *) splitInfo.promotedPKeyP + INT_FIELD_LEN, sizeof(int));
                    pKeypLen += varLen;
                }
                if (freeSpc >= pKeypLen) {   // insert into the current inter node
                    splitInfo.isSplit = false;
                    formInterNodePageData(page, attribute, splitInfo, pKeypLen, PAGE_SIZE - freeSpc - INT_FIELD_LEN, newPage);
                    res = ixFileHandle.fh.writePage(currentNode, newPage);
                    if(res != 0) {return -1; }
                } else { //split the current node
                    //Todo 改分割方式
                    splitInfo.isSplit = true;
                    int splitOffset = getSplitOffset_internal(attribute, page, freeSpc); //到right node结束
                    if (splitTheInternalNode(ixFileHandle, attribute, currentNode, splitInfo, page,
                            freeSpc, pKeypLen, splitOffset)!= 0) {return -1;}
                }
            }
        }
        return 0;
    }


    int IndexManager::splitTheInternalNode(IXFileHandle &ixFileHandle, const Attribute &attribute, int currentNode,
                                            SplitInfo &splitInfo, //void *pKeypToInsert,
                                            void *page, int freeSpcLen, int pKeypLen, int splitOffset) const {
        char keyToInsert[PAGE_SIZE];
        int keyToInsertLen = sizeof(int);
        if(attribute.type == TypeVarChar) {
            memcpy(&keyToInsertLen, (char*)splitInfo.promotedPKeyP + sizeof(int), sizeof(int));
            keyToInsertLen += sizeof(int);
        }
        memcpy(keyToInsert, (char*)splitInfo.promotedPKeyP + sizeof(int), keyToInsertLen);
        char leftIndexPage[PAGE_SIZE];
        char rightIndexPage[PAGE_SIZE];
        int freeSpc_LeftIndex = PAGE_SIZE - splitOffset - INT_FIELD_LEN;
//form left index page
        memcpy(leftIndexPage, page, splitOffset);
        updateFreeSpc(leftIndexPage, freeSpc_LeftIndex);

        int freeSpc_RightIndex = PAGE_SIZE - (PAGE_SIZE - INT_FIELD_LEN - freeSpcLen - splitOffset) - INT_FIELD_LEN * 2;
        memcpy(rightIndexPage, (char*)page + splitOffset - INT_FIELD_LEN,
               PAGE_SIZE - INT_FIELD_LEN - freeSpc_RightIndex);
        updateFreeSpc(rightIndexPage, freeSpc_RightIndex);

        char firstKeyInTheRightPage[PAGE_SIZE];
        int varLen = getKeyLen((char*)rightIndexPage + INT_FIELD_LEN, attribute);

        memcpy(firstKeyInTheRightPage, (char*)rightIndexPage + INT_FIELD_LEN, varLen);
        char leftIndexPage2[PAGE_SIZE];
        char rightIndexPage2[PAGE_SIZE];
        if(compareKey(keyToInsert, firstKeyInTheRightPage, attribute) >= 0) {
            formInterNodePageData(rightIndexPage, attribute, splitInfo, pKeypLen,
                                  PAGE_SIZE - INT_FIELD_LEN - freeSpc_RightIndex, rightIndexPage2);
            memcpy(leftIndexPage2, leftIndexPage, PAGE_SIZE);
        }else {
            formInterNodePageData(leftIndexPage, attribute, splitInfo, pKeypLen,
                                  PAGE_SIZE - INT_FIELD_LEN - freeSpc_LeftIndex, leftIndexPage2);
            memcpy(rightIndexPage2, rightIndexPage, PAGE_SIZE);
        }

        int rightIndexPageNum = ixFileHandle.fh.getNumberOfPages();
        int len_firstKeyInTheRightPage = getKeyLen((char*)rightIndexPage2 + INT_FIELD_LEN, attribute);
        memcpy((char*)splitInfo.promotedPKeyP, &currentNode, INT_FIELD_LEN);
        memcpy((char*)splitInfo.promotedPKeyP + INT_FIELD_LEN, (char*)rightIndexPage2 + INT_FIELD_LEN, len_firstKeyInTheRightPage);
        memcpy((char*)splitInfo.promotedPKeyP + INT_FIELD_LEN + len_firstKeyInTheRightPage, &rightIndexPageNum, INT_FIELD_LEN);

        char newRightIndexPage[PAGE_SIZE];
        int tempFreeSpcLen = getFreeSpc(rightIndexPage2); //not freeSpcIndicator, since it is a intermediate node
        memcpy(newRightIndexPage, (char*)rightIndexPage2 + INT_FIELD_LEN + len_firstKeyInTheRightPage,
               PAGE_SIZE - tempFreeSpcLen - 2 * INT_FIELD_LEN - len_firstKeyInTheRightPage);
        updateFreeSpc(newRightIndexPage, tempFreeSpcLen + INT_FIELD_LEN + len_firstKeyInTheRightPage);
        if(ixFileHandle.fh.writePage(currentNode, leftIndexPage2)!=0){return -1;}
        if(ixFileHandle.fh.appendPage(newRightIndexPage)!=0){return -1;}

        ///////////////////
//        cout<<"splitTheInternalNode: insert into the current inter node--left"<<endl;
//        printPage(leftIndexPage2);
//        cout<<"splitTheInternalNode: insert into the current inter node--right"<<endl;
//        printPage(newRightIndexPage);
        ///////////////////
    }

    int IndexManager::getFreeSpc(void *page) const {
        int freeSpaceLen = getFreeSpcIndicator(page);
        if(freeSpaceLen >= pow(2, 14)) { //is leaf node?
            freeSpaceLen -= pow(2, 14);
        }
        return freeSpaceLen;
    }

    int IndexManager::getFreeSpcIndicator(const void *page) const {
        int freeSpcIndicator;
        memcpy(&freeSpcIndicator, (char *) page + PAGE_SIZE - INT_FIELD_LEN, INT_FIELD_LEN);
        return freeSpcIndicator;
    }


    int IndexManager::getSplitOffset_internal(const Attribute &attribute, const void *page, int freeSpcLen) const {
        int splitOffset = INT_FIELD_LEN;
        if(attribute.type == TypeVarChar){
            int tempInt = -1;
            while(splitOffset < ceil((double)(PAGE_SIZE - INT_FIELD_LEN) / 2)) {
                memcpy(&tempInt, (char*)page + splitOffset, INT_FIELD_LEN);
                splitOffset = splitOffset + 2 * INT_FIELD_LEN + tempInt;
            }
        }else {//减掉第一个page num
            int numOfTotalRecords = (PAGE_SIZE - freeSpcLen - 2 * INT_FIELD_LEN) / (2 * INT_FIELD_LEN);
            int recordsInFirstPage = ceil((double)numOfTotalRecords / 2);
            splitOffset = recordsInFirstPage * 2 * INT_FIELD_LEN + INT_FIELD_LEN; //加上第一个page num
        }
        return splitOffset; //到right node 结束
    }

    int IndexManager::formInterNodePageData(void *page, Attribute attribute, SplitInfo splitInfo,
                                            int pKeypPairLen, int lenOfTheRecords, void * newPage) const {
        char key_varchar[PAGE_SIZE];
        if(attribute.type == TypeInt || attribute.type == TypeReal) {memcpy(key_varchar, (char*)splitInfo.promotedPKeyP + INT_FIELD_LEN, INT_FIELD_LEN);
        }else {memcpy(key_varchar, (char*)splitInfo.promotedPKeyP + INT_FIELD_LEN, pKeypPairLen - 2 * INT_FIELD_LEN);}

        char tempKey[PAGE_SIZE];
        int offset = sizeof(int);
        bool isPosFound = false;
        int tempKeyLen = -1;
        while(offset < lenOfTheRecords) {
            tempKeyLen = getKeyLen((char*)page + offset, attribute);
            memcpy(tempKey, (char*)page + offset, tempKeyLen);
            if(compareKey(tempKey, key_varchar, attribute) > 0) {//加在temp key的前面
                isPosFound = true;
                break;
            }
            offset = offset + sizeof(int) + tempKeyLen;
        }
        if(isPosFound) {
            memcpy(newPage, (char *) page, offset - sizeof(int));
            memcpy((char *) newPage + offset - sizeof(int), (char*)splitInfo.promotedPKeyP, pKeypPairLen);
            memcpy((char *) newPage + offset - sizeof(int) + pKeypPairLen,
                   (char *) page + offset, lenOfTheRecords - offset);
        }else { //add at the last position
            memcpy(newPage, (char*)page, lenOfTheRecords);
            memcpy((char*)newPage + lenOfTheRecords - sizeof(int), (char*)splitInfo.promotedPKeyP, pKeypPairLen);
        }
        int freeSpcLen = getFreeSpcIndicator(page) - pKeypPairLen + INT_FIELD_LEN;
        updateFreeSpc(newPage, freeSpcLen);

        return 0;
    }


    int IndexManager::getKeyLen(void *key, const Attribute &attribute) const {
        int tempKeyLen = sizeof(int);
        if(attribute.type == TypeVarChar){
            memcpy(&tempKeyLen, (char*)key, sizeof(int));
            tempKeyLen += sizeof(int);
        }
        return tempKeyLen;
    }

    int
    IndexManager::getSubTreePageNum(const Attribute &attribute, const void* key, const void *page,
                                    int lenOfAllRecords, bool isKeyNull) const {
        int subTreePageNum= -1, len = -1;
        bool isSubTreeFound = false;
        char tempK[PAGE_SIZE];

        int offset = sizeof(int);
        while(offset < lenOfAllRecords){ //inter-node has no next leaf node pointer
            len = sizeof(int);
            if(attribute.type == TypeVarChar) {
                memcpy(&len, (char*)page + offset, sizeof(int));
                len += sizeof(int);
            }
            memcpy(tempK, (char*)page + offset, len);
            if((!isKeyNull && compareKey(tempK, (char*)key, attribute) >= 0) || isKeyNull) {
                memcpy(&subTreePageNum, (char*)page + offset - sizeof(int), sizeof(int));
                isSubTreeFound = true;
                break;
            }
            offset = offset + sizeof(int) + len;
        }
        if(!isSubTreeFound) {
            //last subtree
            memcpy(&subTreePageNum, (char*)page + offset - sizeof(int), sizeof(int));
        }
        return subTreePageNum;
    }


    RC IndexManager::splitLeafNode(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                                     int currentNode, SplitInfo &splitInfo, const void *keyRidPair,
                                     int keyRidPairLen, void *page, int freeSpcLen,
                                     int splitOffset) {
        char leftPage[PAGE_SIZE];
        char rightPage[PAGE_SIZE];

        int freeSpace_LeftPage = PAGE_SIZE - splitOffset - 2 * INT_FIELD_LEN;
        int freeSpace_RightPage = PAGE_SIZE - (PAGE_SIZE - 2 * INT_FIELD_LEN - freeSpcLen - splitOffset) - 2 * INT_FIELD_LEN;
        int record_length_left = PAGE_SIZE - 2 * INT_FIELD_LEN - freeSpace_LeftPage;
        int record_length_right = PAGE_SIZE - 2 * INT_FIELD_LEN - freeSpace_RightPage;
        freeSpace_LeftPage += pow(2, 14); //add flag for leaf node
        freeSpace_RightPage += pow(2, 14);

        int nextPagePointer_LeftPage = ixFileHandle.fh.getNumberOfPages();
        int currentNextPage = getNextPageID(page);
        int nextPagePointer_RightPage = currentNextPage;

        memcpy(leftPage, page, splitOffset);
        updateFreeSpc(leftPage, freeSpace_LeftPage);
        updateNextPagePointer(leftPage, nextPagePointer_LeftPage);

        memcpy(rightPage, (char*)page + splitOffset,
               (getLenOfRecords(freeSpcLen) - splitOffset));
        updateFreeSpc(rightPage, freeSpace_RightPage);
        updateNextPagePointer(rightPage, nextPagePointer_RightPage);

        char firstKeyInTheRightPage[PAGE_SIZE];
        int varLen = sizeof(int);
        if(attribute.type == TypeVarChar){
            memcpy(&varLen, rightPage, sizeof(int));
            varLen += sizeof(int);
        }
        memcpy(firstKeyInTheRightPage, rightPage, varLen);
        char rightPageAfterInsert[PAGE_SIZE];
        char leftPageAfterInsert[PAGE_SIZE];
        if(compareKey((char*)key, firstKeyInTheRightPage, attribute) >= 0) {
            formLeafNodeData(rightPage, attribute, keyRidPair, keyRidPairLen,
                             record_length_right, rightPageAfterInsert);
            memcpy(leftPageAfterInsert, leftPage, PAGE_SIZE);
        }else {
            formLeafNodeData(leftPage, attribute, keyRidPair, keyRidPairLen,
                             record_length_left, leftPageAfterInsert);
            memcpy(rightPageAfterInsert, rightPage, PAGE_SIZE);
        }

        int keyLen = 0;
        if(attribute.type == TypeVarChar){
            memcpy(&keyLen, rightPageAfterInsert, sizeof(int));
        }
        keyLen += sizeof(int);

        memcpy((char*)splitInfo.promotedPKeyP + INT_FIELD_LEN, rightPageAfterInsert, keyLen);
        memcpy((char*)splitInfo.promotedPKeyP, &currentNode, INT_FIELD_LEN);
        memcpy((char*)splitInfo.promotedPKeyP + keyLen + INT_FIELD_LEN, &nextPagePointer_LeftPage, INT_FIELD_LEN);
        if(ixFileHandle.fh.writePage(currentNode, leftPageAfterInsert)!=0){return -1;}
        if(ixFileHandle.fh.appendPage(rightPageAfterInsert)!=0){return -1;}
        return 0;
    }

    int IndexManager::updateNextPagePointer(char *page, int nextPagePointer) const {
        memcpy((char*)page + PAGE_SIZE - 2 * INT_FIELD_LEN, &nextPagePointer, INT_FIELD_LEN);
    }

    int IndexManager::getNextPageID(void *page) const {
        int nextPage = -1;
        memcpy(&nextPage, (char*)page + PAGE_SIZE - 2 * INT_FIELD_LEN, INT_FIELD_LEN);
        return nextPage;
    }


    int IndexManager::getSplitOffset_leaf(const Attribute &attribute, const void *page,
                                          int freeSpc) const {
        int splitOffset = 0;
        if(attribute.type == TypeVarChar){
            int tempInt = -1;
            while(splitOffset < ceil((double)(PAGE_SIZE - 2 * INT_FIELD_LEN) / 2)) {
                memcpy(&tempInt, (char*)page + splitOffset, INT_FIELD_LEN);
                splitOffset = splitOffset + 3 * sizeof(int) + tempInt;
            }
        }else {
            int numOfTotalRecords = this->getLenOfRecords(freeSpc) / (3 * INT_FIELD_LEN);
            int recordsInFirstPage = ceil((double)numOfTotalRecords / 2);
            splitOffset = recordsInFirstPage * (3 * INT_FIELD_LEN);
        }
        return splitOffset;
    }

    int IndexManager::formLeafNodeData(void *oldPage, Attribute attribute, const void *keyRidPair,
                                       int keyRidPairLen, int lenOfTheRecords, void * newPage) const {
        char key[PAGE_SIZE];
        int tempLen = sizeof(int);
        if(attribute.type == TypeVarChar){
            memcpy(&tempLen, keyRidPair, sizeof(int));
            tempLen += sizeof(int);
        }
        memcpy(key, keyRidPair, tempLen);

        int offset = 0;
        while(offset < lenOfTheRecords) {
            char tempKey[PAGE_SIZE];
            tempLen = 0;
//            cout<<*(char*)oldPage + offset<<", "<<*(char*)key<<endl;
            if(attribute.type == TypeVarChar){
                memcpy(&tempLen, (char*)oldPage + offset, sizeof(int));
            }
            tempLen += sizeof(int);
            memcpy(tempKey, (char*)oldPage + offset, tempLen);
            if(compareKey(key, tempKey, attribute) < 0) {
                break;
            }
            offset = offset + tempLen + 2 * sizeof(int);
        }

        memcpy(newPage, (char*)oldPage, offset);
        memcpy((char*)newPage + offset, keyRidPair, keyRidPairLen);
        memcpy((char*)newPage + offset + keyRidPairLen, (char*)oldPage + offset, lenOfTheRecords - offset);

        updateNextPagePointer((char*)newPage, this->getNextPageID(oldPage));
        updateFreeSpc(newPage, getFreeSpcIndicator(oldPage) - keyRidPairLen);

        return 0;
    }



    int IndexManager::compareKey(void* key1, void *key2, Attribute attribute) const {
        int comp = 0;
        if(attribute.type == TypeInt) {
            int key1_int = -1, key2_int = -1;
            memcpy(&key1_int, key1, sizeof(int));
            memcpy(&key2_int, key2, sizeof(int));
            if(key1_int - key2_int > 0) {comp = 1; }
            else if(key1_int - key2_int < 0) {comp = -1; }
            else if(key1_int - key2_int == 0) {comp == 0; }
        }
        else if(attribute.type == TypeReal) {
            float key1_float = 0.0, key2_float = 0.0;
            memcpy(&key1_float, key1, sizeof(int));
            memcpy(&key2_float, key2, sizeof(int));
            if((fabs(key1_float - key2_float) < 1E-6)) {comp = 0; }
            else if(key1_float < key2_float
                    && fabs(key1_float - key2_float) > 1E-6) {comp = -1; }
            else if(key1_float > key2_float
                    && fabs(key1_float - key2_float) > 1E-6){comp = 1; }
        }
        else if (attribute.type == TypeVarChar) {
            int len1 = 0, len2 = 0;
            memcpy(&len1, key1, sizeof(int));
            memcpy(&len2, key2, sizeof(int));
            string k1((char *)key1 + sizeof(int), len1);
            string k2((char *)key2 + sizeof(int), len2);
            comp = strcmp(k1.c_str(), k2.c_str());
        }
        return comp;
    }

//    int IndexManager::markAsLeaf(IXFileHandle &ixFileHandle, const RID &newRid) const {
//        char pageData[PAGE_SIZE];
//        if(ixFileHandle.fh.readPage(newRid.pageNum, pageData) !=0 ){return -1;}
//        int freeSpc = rbfm.getFreeSpc(pageData) + pow(2, 14);
//        rbfm.updateFreeSpc(pageData, freeSpc);
//        if(ixFileHandle.fh.writePage(newRid.pageNum, pageData)!=0) {return -2;}
//        return 0;
//    }
//todo remove freeSpc
    bool IndexManager::isLeafNode(const void *page, int &freeSpc) const {
        freeSpc = getFreeSpcIndicator((char*)page);
        if(freeSpc >= pow(2, 14)) {  //leaf
            freeSpc -= pow(2, 14);
            return true;
        }
        return false;
    }

//todo:
    int IndexManager::formKeyRidPair_returnLength(const Attribute &attribute, const void *key, const RID &rid,
                                                  void *keyRidPair) {
        int offset = 0, length = 0;
        if(attribute.type == TypeVarChar) {
            int varcharLen;
            memcpy(&varcharLen, key, sizeof(int));
            memcpy(keyRidPair, key, varcharLen + sizeof(int));
            offset = offset + varcharLen + sizeof(int);
        }else {
            memcpy(keyRidPair, key, sizeof(int));
            offset += sizeof(int);
        }

        memcpy((char*)keyRidPair + offset, &rid.pageNum, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)keyRidPair + offset, &rid.slotNum, sizeof(int));
        offset += sizeof(int);
        return offset;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        IX_ScanIterator ix_ScanIterator;
        if (scan(ixFileHandle, attribute, key, key, true, true, ix_ScanIterator)!= 0){return -1;}
        RID returnedRID;
        int counter = 0;
        char returnedKey[PAGE_SIZE];
        int keyRID_len = 0, keyLen = 0;
//        if(rid.pageNum == 270265) {
//            cout<<"aaaa"<<endl;
//        }
        while (ix_ScanIterator.getNextEntry(returnedRID, &returnedKey) == 0) {
            if(returnedRID.pageNum == rid.pageNum && returnedRID.slotNum == rid.slotNum) {
                counter++;
                keyRID_len = this->getKeyLen((char *) key, attribute) + 2 * INT_FIELD_LEN;
                ix_ScanIterator.recordOffset -= keyRID_len;
                char newPage[PAGE_SIZE];
                memcpy(newPage, ix_ScanIterator.page, ix_ScanIterator.recordOffset);
                memcpy((char *) newPage + ix_ScanIterator.recordOffset,
                       (char *) ix_ScanIterator.page + ix_ScanIterator.recordOffset + keyRID_len,
                       getLenOfRecords(ix_ScanIterator.freeSpc) - ix_ScanIterator.recordOffset - keyRID_len);
                ix_ScanIterator.freeSpc += keyRID_len;
                updateFreeSpc(newPage, getFreeSpcIndicator(ix_ScanIterator.page) + keyRID_len);
                updateNextPagePointer(newPage, getNextPageID(ix_ScanIterator.page));
                if (ixFileHandle.fh.writePage(ix_ScanIterator.currentNode, newPage) != 0) { return -1; }
                memcpy(ix_ScanIterator.page, newPage, PAGE_SIZE);
//            UPDATE internal node
//            if(ix_ScanIterator.recordOffset == 0 && ) { //todo :get num of pages
//                if(ixFileHandle.fh.readPage(ix_ScanIterator.lastNode, newPage) == 0) {
//                    int offset = sizeof(int);
//                    char tempKey[PAGE_SIZE], objectedKey[PAGE_SIZE];
//                    while(offset < getLenOfRecords(getFreeSpc(newPage))){ //inter-node has no next leaf node pointer
//                        keyLen = getKeyLen(newPage + offset, attribute);
//                        memcpy(tempKey, (char*)newPage + offset, keyLen);
//                        if(compareKey(tempKey, (char*)key, attribute) >= 0) {
//                            break;
//                        }
//                        offset = offset + sizeof(int) + keyLen;
//                    }
//                    char firstKey[PAGE_SIZE];
//                    int firstKeyLen = getKeyLen((char*)newPage + INT_FIELD_LEN, attribute);
//                    memcpy(firstKey, (char*)newPage + INT_FIELD_LEN, firstKeyLen);
//                    if(compareKey(firstKey, tempKey, attribute) != 0) {
//                        char newPage2[PAGE_SIZE];
//                        memcpy(newPage2, newPage, offset);
//                        memcpy(newPage2 + offset, firstKey, firstKeyLen);
//                        int freeSpc_internal = getFreeSpc(newPage);
//                        int recordsLen = getLenOfRecords(freeSpc_internal) + INT_FIELD_LEN;
//                        memcpy(newPage2 + offset + firstKeyLen, newPage + offset + keyLen, recordsLen - offset - keyLen);
//                        updateFreeSpc(newPage2, getFreeSpc(newPage) + keyLen - firstKeyLen);
//                        if(ixFileHandle.fh.writePage(ix_ScanIterator.lastNode, newPage2)!= 0){return -1;}
//                    }
//                }
//            }
            }


        }
        return counter > 0 ? 0: -1;
    }
// todo: get root
    int IndexManager::getRoot(IXFileHandle &ixFileHandle) const {
        int root = ixFileHandle.fh.root;
        ixFileHandle.ixReadPageCounter++;
        ixFileHandle.fh.readPageCounter++;
        return root;
    }

    void IndexManager::updateFreeSpc(void *pageData, int val) const {
        memcpy((char *) pageData + PAGE_SIZE - INT_FIELD_LEN, &val, INT_FIELD_LEN);
    }

    /**
 * Using key and rid to search the position of the record;
 * if key == NULL, return the first satisfied key-rid pair
 * @param ixFileHandle
 * @param attribute
 * @param key
 * @param rid
 * @param currentNode
 * @param offset
 * @param length
 * @param node
 * @return
 */

    //todo: rewrite
//    RC IndexManager::searchAndDeleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
//                                          const void *key, const RID &rid, int currentNode) {
//        char page[PAGE_SIZE];
//        if(ixFileHandle.fh.readPage(currentNode, page) != 0) {return -1;}
//        int freeSpcLen = -1, offset = 0, length = 0;
//
//        if(isLeafNode(page, freeSpcLen)) { //get the offset and length in the current page
//            int tempPageNum = -1, tempSlotNum = -1, tempOffset = 0, varcharLen = 0;
//            char tempKey[PAGE_SIZE];
//            while(tempOffset < PAGE_SIZE - freeSpcLen - 2 * INT_FIELD_LEN){
//                if(attribute.type == TypeReal || attribute.type == TypeInt) {
//                    memcpy(tempKey, (char*)page + tempOffset, sizeof(int));
//                }else {
//                    memcpy(&varcharLen, (char*)page + tempOffset, sizeof(int));
//                    memcpy(tempKey, (char*)page + tempOffset, sizeof(int) + varcharLen);
//                    tempOffset += varcharLen;
//                }
//                tempOffset += sizeof(int);
//                if(compareKey(tempKey, (char*)key, attribute) == 0) {
//                    memcpy(&tempPageNum, (char*)page + tempOffset, sizeof(int));
//                    tempOffset += sizeof(int);
//                    memcpy(&tempSlotNum, (char*)page + tempOffset, sizeof(int));
//                    tempOffset += sizeof(int);
//                    if((tempPageNum == rid.pageNum) && (tempSlotNum == rid.slotNum)) {
//                        length = 3 * sizeof(int);
//                        if(attribute.type == TypeVarChar) {length += varcharLen; }
//                        offset = tempOffset - length;
//                        char newPage[PAGE_SIZE];
//                        memcpy(newPage, page, offset);
//                        memcpy((char*)newPage + offset,                        (char*)page + offset + length, PAGE_SIZE - offset - length - 2 * sizeof(int));
//                        int freeSpc = getFreeSpcIndicator(page) + length;
//                        updateFreeSpc(newPage, freeSpc);
//                        memcpy((char*)newPage + PAGE_SIZE - 2 * sizeof(int), (char*)page + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
//                        return ixFileHandle.fh.writePage(currentNode, newPage);
//                    }
//                }else {tempOffset += 2 * sizeof(int); }
//            }
//        } else {
//            int subTreePageNum = getSubTreePageNum(attribute, key, page, PAGE_SIZE - INT_FIELD_LEN - freeSpcLen, false);
//            if(subTreePageNum == -1) {return -1;}
//            return searchAndDeleteEntry(ixFileHandle, attribute, key, rid, subTreePageNum);
//        }
//
//        return -2;
//    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle, const Attribute &attribute,
                          const void *lowKey, const void *highKey,
                          bool lowKeyInclusive, bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        if(!pfm.isFileExisting(ixFileHandle.fh.fileName)) {return -1; } //file does not exist
        if(lowKey == NULL) { ix_ScanIterator.lowKeyInclusive = 0;
        }else if (lowKeyInclusive){ ix_ScanIterator.lowKeyInclusive = 1;
        }else { ix_ScanIterator.lowKeyInclusive = -1;}

        if(highKey == NULL) { ix_ScanIterator.highKeyInclusive = 0;
        }else if (highKeyInclusive){ ix_ScanIterator.highKeyInclusive = 1;
        }else {ix_ScanIterator.highKeyInclusive = -1; }

        ix_ScanIterator.recordOffset = 0;
        ix_ScanIterator.lastNode = -1;

        ix_ScanIterator.ixFileHandle = &ixFileHandle;
        ix_ScanIterator.attribute = attribute;
        ix_ScanIterator.rootNode = getRoot(ixFileHandle);
        ix_ScanIterator.currentNode = ix_ScanIterator.rootNode;
        ix_ScanIterator.recordOffset = 0;
        ix_ScanIterator.isIteratorNew = true;

        if(ix_ScanIterator.rootNode == -1) {return -1; }
//        ix_ScanIterator.currentNode = -1;
        if(attribute.type == TypeReal || attribute.type == TypeInt) {
            if(lowKey != NULL) {
                memcpy(ix_ScanIterator.lowKey, lowKey, sizeof(int));
            }
            if(highKey != NULL) {
                memcpy(ix_ScanIterator.highKey, highKey, sizeof(int));
            }
        }else {
            int varcharLen = -1;
            if(lowKey != NULL) {
                memcpy(&varcharLen, lowKey, sizeof(int));
                memcpy(ix_ScanIterator.lowKey, lowKey, sizeof(int) + varcharLen);
            }
            if(highKey != NULL) {
                memcpy(&varcharLen, highKey, sizeof(int));
                memcpy(ix_ScanIterator.highKey, highKey, sizeof(int) + varcharLen);
            }
        }

        return 0;
    }


    bool IX_ScanIterator::isLowKeySatisfied(void* currentKey) {
        if(lowKeyInclusive == 0) {
            return true;
        }
        if(attribute.type == TypeInt) {
            int keyInt = -1, lowKeyInt = -1;
            memcpy(&keyInt, currentKey, sizeof(int));
            memcpy(&lowKeyInt, lowKey, sizeof(int));
            if(lowKeyInclusive == 1 && lowKeyInt <= keyInt) {return true;}
            if(lowKeyInclusive == -1 && lowKeyInt < keyInt) {return true;}
        }
//    cout<<"lowKey: "<<this->lowKey<<", highKey: "<<this->highKey<<", tempKey = "<<currentKey<<endl;
        if(attribute.type == TypeReal) {
            float keyReal, lowKeyReal;
            memcpy(&keyReal, currentKey, sizeof(float));
            memcpy(&lowKeyReal, this->lowKey, sizeof(float));
//            cout<<keyReal<<", "<<lowKeyReal<<endl;
//        cout<<"current key = "<<keyReal<<", lowKeyReal = "<<lowKeyReal<<endl;
            if(lowKeyInclusive == 1 && ((lowKeyReal < keyReal
                                         && fabs(lowKeyReal - keyReal) > 1E-6) || (fabs(lowKeyReal - keyReal) < 1E-6))
                    ) {return true;}
            if(lowKeyInclusive == -1 && lowKeyReal < keyReal
               && fabs(lowKeyReal - keyReal) > 1E-6) {return true;}
        }
        if(attribute.type == TypeVarChar) {
            int varcharLen = 0, len = 0;
            memcpy(&varcharLen, this->lowKey, sizeof(int));
            memcpy(&len, (char*)currentKey, sizeof(int));
            string keyStr((char *)currentKey + sizeof(int), len);
            string lowKeyStr((char *)this->lowKey + sizeof(int), varcharLen);
            int res = strcmp(keyStr.c_str(), lowKeyStr.c_str());
            if(lowKeyInclusive == 1 && res >= 0) {return true; }
            if(lowKeyInclusive == -1 && res > 0) {return true; }
        }
        return false;
    }


    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        if(ixFileHandle.fh.getNumberOfPages() > 1) {out<<"{";}
        traverseTree(ixFileHandle, getRoot(ixFileHandle), attribute, 0, out);
        if(ixFileHandle.fh.getNumberOfPages() > 1) {out<<endl<<"}"<<endl;}
        return 0;
    }

    void IndexManager::printStr(int varcharLen, const char *strValue, std::ostream &out) const {
        if(varcharLen != 0) {
//    for (int i = 0; i < varcharLen-1; i++) {
            for (int i = 0; i < varcharLen; i++) {
//        printByte(*(byte *) (strValue + i));
                out << *(char *) (strValue + i);
            }
        }else {out<<"NULL";}
//    cout<<endl;
    }


    int IndexManager::traverseTree(IXFileHandle &ixFileHandle, int node, Attribute attribute, int layer, std::ostream &out) const{
        char page[PAGE_SIZE];
        if(ixFileHandle.fh.readPage(node, page)!=0){return -1;}
        int freeSpcLen = -1;
        int offset = 0;
        int key_int = 0;
        int tempLen = -1;
        float key_float = 0.0;
        char key[PAGE_SIZE];
        int recordLen = -1, keylen = -1, leftNode = -1, rightNode = -1;

        int i;
        if(isLeafNode(page, freeSpcLen)) {
            for(i = 0; i < layer; i++){out<<"  ";}
            out<<"{\"keys\": [";
            int i = 0;
            recordLen = PAGE_SIZE - 2 * sizeof(int) - freeSpcLen;
            offset = 0;
            while(offset < recordLen) {
                int tempPageNum = -1, tempSlotNum = -1;
                if(i > 0) {out<<",";}
                out<<"\"";
                if(attribute.type == TypeVarChar) {
                    memcpy(&tempLen, (char*)page + offset, sizeof(int));
                    printStr(tempLen, (char*)page + offset + sizeof(int), out);
                    offset += tempLen;
                }else if(attribute.type == TypeInt){
                    memcpy(&key_int, (char*)page + offset, sizeof(int));
                    out<<key_int;
                }else {
                    memcpy(&key_float, (char*)page + offset, sizeof(int));
                    out<<key_float;
                }

                offset += sizeof(int);
                memcpy(&tempPageNum, (char*)page + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&tempSlotNum, (char*)page + offset, sizeof(int));
                offset += sizeof(int);
                out<<":[("<<tempPageNum<<","<<tempSlotNum<<")]\"";
                i++;
            }
            out<<"]}";
        }
        else {      //if(node == ixFileHandle.rootNode){
            recordLen = PAGE_SIZE - freeSpcLen - INT_FIELD_LEN;
            offset = sizeof(int);
            for(i = 0; i < layer; i++){out<<"  ";}
            out<<"\"keys\":[";
            int i = 0;
            while(offset < recordLen){
                memcpy(&leftNode, (char*)page + offset - sizeof(int), sizeof(int));
                keylen = sizeof(int);
                if(attribute.type == TypeVarChar) {
                    memcpy(&keylen, (char*)page + offset, sizeof(int));
                    offset += sizeof(int);
                    memcpy(key, (char*)page + offset, keylen);
                    if(i > 0) {out<<",";}
                    out<<"\"";
                    printStr(keylen, (char*)key, out);
                    out<<"\"";
                    offset += keylen;
                }else if(attribute.type == TypeInt){
                    memcpy(&key_int, (char*)page + offset, sizeof(int));
                    if(i > 0) {out<<",";}
                    out<<"\""<<key_int<<"\"";
                    offset += sizeof(int);
                }else if(attribute.type == TypeReal) {
                    memcpy(&key_float, (char*)page + offset, sizeof(float));
                    if(i > 0) {out<<",";}
                    out<<"\""<<key_float<<"\"";
                    offset += sizeof(int);
                }
                offset += sizeof(int);
                i++;
            }
            out<<"],"<<endl;
            for(i = 0; i < layer; i++) {out<<" ";}
            out<<"\"children\":["<<endl;
            offset = sizeof(int);
            layer++;

            while(offset < recordLen){
                memcpy(&leftNode, (char*)page + offset - sizeof(int), sizeof(int));
                if(attribute.type == TypeVarChar) {
                    memcpy(&keylen, (char*)page + offset, sizeof(int));
                    offset += keylen;
                }
                offset += sizeof(int);
                memcpy(&rightNode, (char*)page + offset, sizeof(int));
                offset += sizeof(int);

                traverseTree(ixFileHandle, leftNode, attribute, layer, out);
                out<<","<<endl;
            }
            traverseTree(ixFileHandle, rightNode, attribute, layer, out);
            out<<endl<<"]";
        }
    }
    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }


//    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
////        char returnedKey[PAGE_SIZE];
//        //come to the end of the file.
//        if(this->currentNode == -1) {return IX_EOF;}
//        if(currentNode != lastNode) {
//            if(ixFileHandle->fh.readPage(currentNode, page)!=0){return -1;}
//            freeSpc = im.getFreeSpc(page);
////            cout<<im.getNextPageID(page)<<endl;
//        }
//
//        int keyLen = getKeyLength((char*) page + recordOffset, attribute.type);
//        memcpy(key, (char*)page + recordOffset, keyLen);
//
//        if(!isHighKeySatisfied(key)){return -1; }
//        recordOffset += keyLen;
//        int slotNum = 0;
//        memcpy(&rid.pageNum, (char*)page + recordOffset, sizeof(int));
//        recordOffset += sizeof(int);
//        memcpy(&slotNum, (char*)page + recordOffset, sizeof(int));
//        rid.slotNum = (short)slotNum;
//        recordOffset += sizeof(int);
//        lastNode = currentNode;
//
//        if(recordOffset >= PAGE_SIZE - freeSpc - 2 * sizeof(int)){
//            recordOffset = 0;
//            currentNode = im.getNextPageID(page);
////            cout<<currentNode<<endl;
//        }
////        cout<<"rid:"<<rid.pageNum<<", "<<rid.slotNum<<endl;
//        return 0;
//    }

//    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
//        int res = -1;
//        //come to the end of the file.
//        if(this->currentNode == -1) {return IX_EOF;}
//        if(currentNode != lastNode) {
//            if(ixFileHandle->fh.readPage(currentNode, page)!=0){return -1;}
//            freeSpc = im.getFreeSpc(page);
//            lastNode = currentNode;
////            cout<<im.getNextPageID(page)<<endl;
//        }
////        char returnedKey[PAGE_SIZE];
//        if(isIteratorNew) {
//            isIteratorNew = false;
////            currentNode = rootNode;
//
//            res = searchFirstEntry(key, rid);
//            if(res == -1) {
//                return -1;
//            }
//            ////////////////
////            int temp;
////            memcpy(&temp, returnedKey, 4);
//
////            int keyLen = sizeof(int);
////            if(attribute.type == TypeVarChar){
////                memcpy(&keyLen, returnedKey, sizeof(int));
////                keyLen += sizeof(int);
////            }
////            memcpy(key, returnedKey, keyLen);
////            res = 0;
//        }else {
//            int keyLen = getKeyLength((char*) page + recordOffset, attribute.type);
//            memcpy(key, (char*)page + recordOffset, keyLen);
//
//            if(!isHighKeySatisfied(key)){return -1; }
//            recordOffset += keyLen;
//            int slotNum = 0;
//            memcpy(&rid.pageNum, (char*)page + recordOffset, sizeof(int));
//            recordOffset += sizeof(int);
//            memcpy(&slotNum, (char*)page + recordOffset, sizeof(int));
//            rid.slotNum = (short)slotNum;
//            recordOffset += sizeof(int);
//            lastNode = currentNode;
//            res = 0;
//
//        }
//        if(recordOffset >= im.getLenOfRecords(freeSpc) -1){
//            recordOffset = 0;
//            lastNode = currentNode;
//            currentNode = im.getNextPageID(page);
//        }
//        return res;
//    }

//todo
    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        int res = -1;
        char returnedKey[PAGE_SIZE];
        if(isIteratorNew) {
            isIteratorNew = false;
            currentNode = rootNode;
            res = searchFirstEntry(returnedKey, rid);
            if(res != 0) {
                return -1;
            }
        }else {
            res = searchNextLeafEntry(returnedKey, rid);
            if(res == -1) {
                return -1;
            }
        }
        int keyLen = sizeof(int);
        if(attribute.type == TypeVarChar){
            memcpy(&keyLen, returnedKey, sizeof(int));
            keyLen += sizeof(int);
        }
        if(res == 0) {
            memcpy(key, returnedKey, keyLen);
        }
        return res;
    }


    RC IX_ScanIterator::searchNextLeafEntry(void *returnedKey, RID &rid) {
        //come to the end of the file.
        if(recordOffset >= im.getLenOfRecords(freeSpc)){
            recordOffset = 0;
            lastNode = currentNode;
            currentNode = im.getNextPageID(page);
        }
        if(this->currentNode == -1) {return -1;}
        if(currentNode != lastNode) {
            if(ixFileHandle->fh.readPage(currentNode, page)!=0){return -1;}
            freeSpc = im.getFreeSpc(page);
//            cout<<im.getNextPageID(page)<<endl;
        }
        //get the offset and length in the current page
        char tempKey[PAGE_SIZE];
        int keyLen = sizeof(int);
        if(attribute.type == TypeVarChar) {
            memcpy(&keyLen, (char*)page + recordOffset, sizeof(int));
            keyLen += sizeof(int);
        }
        memcpy(tempKey, (char*)page + recordOffset, keyLen);
        if(!isHighKeySatisfied(tempKey)){return -1;}
        memcpy(returnedKey, tempKey, keyLen);
        recordOffset += keyLen;
        memcpy(&rid.pageNum, (char*)page + recordOffset, sizeof(int));
        recordOffset += sizeof(int);
        memcpy(&rid.slotNum, (char*)page + recordOffset, sizeof(int));
        recordOffset += sizeof(int);
        lastNode = currentNode;

        return 0;
    }






    RC IX_ScanIterator::searchFirstEntry(void *returnedKey, RID &rid) {
        int retu2rnValue = -1;
        int freeSpcLen = -1;
        if(currentNode != lastNode) {
            lastNode = currentNode;
            if(ixFileHandle->fh.readPage(currentNode, page)!=0){return -1;}
            freeSpc = im.getFreeSpc(page);
        }
        if(freeSpc >= PAGE_SIZE - 2 * INT_FIELD_LEN) {
            lastNode = currentNode;
            currentNode = im.getNextPageID(page);
            return searchFirstEntry(returnedKey, rid);
        }
        bool isLeaf = im.isLeafNode(page, freeSpcLen);
        if(!isLeaf) {
            bool isKeyNull = lowKeyInclusive == 0? true: false;
            int subTreePageNum = im.getSubTreePageNum(attribute, lowKey, page, im.getLenOfRecords(freeSpc), isKeyNull);
            if(subTreePageNum == -1) {return -1;}
//            lastNode = currentNode;
            currentNode = subTreePageNum;
            return searchFirstEntry(returnedKey, rid);
        } //get the offset and length in the current page
        int tempPageNum = -1, tempSlotNum = -1, tempOffset = 0;
        int lowKeyInt = -1, highKeyInt = -1;
        bool isKeyRidPairFound = false;
        if(lowKeyInclusive != 0){
            memcpy(&lowKeyInt, lowKey, sizeof(int));
        }
        if(highKeyInclusive != 0){
            memcpy(&highKeyInt, highKey, sizeof(int));
        }
        int lenOfAllEntries = im.getLenOfRecords(freeSpc);
        char tempKey[PAGE_SIZE];
        int keyLen = -1;
        while(tempOffset < lenOfAllEntries){
            keyLen = sizeof(int);
            if(attribute.type == TypeVarChar) {
                memcpy(&keyLen, (char*)page +tempOffset, sizeof(int));
                keyLen += sizeof(int);
            }
            memcpy(tempKey, (char*)page + tempOffset, keyLen);
            if(lowKeyInclusive == 0) {
                if(isHighKeySatisfied(tempKey)){
                    isKeyRidPairFound = true;
                    memcpy(returnedKey, (char*)page + tempOffset, keyLen);
                    tempOffset += keyLen;
                    memcpy(&tempPageNum, (char*)page + tempOffset, sizeof(int));
                    tempOffset += sizeof(int);
                    memcpy(&tempSlotNum, (char*)page + tempOffset, sizeof(int));
                    tempOffset += sizeof(int);
                    break;
                }else {return -1; }
            }
            if(isLowKeySatisfied(tempKey) && isHighKeySatisfied(tempKey)){
                isKeyRidPairFound = true;
                memcpy(returnedKey, (char*)page + tempOffset, keyLen);
                tempOffset += keyLen;
                memcpy(&tempPageNum, (char*)page + tempOffset, sizeof(int));
                tempOffset += sizeof(int);
                memcpy(&tempSlotNum, (char*)page + tempOffset, sizeof(int));
                tempOffset += sizeof(int);
                break;
            }
            tempOffset = tempOffset + keyLen + 2 * sizeof(int);
        }
//            todo:remove:
        lastNode = currentNode;
        if(isKeyRidPairFound) {
            rid.pageNum = tempPageNum;
            rid.slotNum = tempSlotNum;
            recordOffset = tempOffset;
            return 0;
        }
        else {//set current node to next page id
            currentNode = im.getNextPageID(page);
            return searchFirstEntry(returnedKey, rid);
        }
    }

    int IndexManager::getLenOfRecords(int freeSpcLen) const {
        return PAGE_SIZE - freeSpcLen - 2 * sizeof(int);
    }


    int IX_ScanIterator::getKeyLength(void *s, AttrType type) const {
        int keyLen = 0;
        if(type == TypeVarChar) {
            memcpy(&keyLen, s, sizeof(int));
        }
        keyLen += sizeof(int);
        return keyLen;
    }


    bool IX_ScanIterator::isHighKeySatisfied(void* currentKey) {
        if(highKeyInclusive == 0) { return true; }
        if(attribute.type == TypeInt) {
            int keyInt = -1, highKeyInt = -1;
            memcpy(&keyInt, currentKey, sizeof(int));
            memcpy(&highKeyInt, highKey, sizeof(int));
            if(highKeyInclusive == 1 && highKeyInt >= keyInt) {return true;}
            if(highKeyInclusive == -1 && highKeyInt > keyInt) {return true;}
        }
        if(attribute.type == TypeReal) {
            float keyReal = -1, highKeyReal = -1;
            memcpy(&keyReal, currentKey, sizeof(int));
            memcpy(&highKeyReal, highKey, sizeof(int));
            if(highKeyInclusive == 1 && ((highKeyReal > keyReal
            &&fabs(highKeyReal - keyReal) > 1E-6)
            || (fabs(highKeyReal - keyReal) < 1E-6))) {return true;}
            if(highKeyInclusive == -1 && highKeyReal > keyReal
               && fabs(highKeyReal - keyReal) > 1E-6) {return true;}
        }
        if(attribute.type == TypeVarChar) {
            int varcharLen = 0, len = 0;
            memcpy(&varcharLen, this->highKey, sizeof(int));
            memcpy(&len, (char*)currentKey, sizeof(int));
            string keyStr((char *)currentKey + sizeof(int), len);
            string highKeyStr((char *)this->highKey + sizeof(int), varcharLen);
            int res = strcmp(highKeyStr.c_str(), keyStr.c_str());
            if(highKeyInclusive == 1 && res >= 0) {return true; }
            if(highKeyInclusive == -1 && res > 0) {return true; }
        }
        return false;
    }


    RC IX_ScanIterator::close() {
        PagedFileManager &pfm = PagedFileManager::instance();
//        pfm.closeFile(this->ixFileHandle->fh);
        return 0;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        this->fh.collectCounterValues(this->ixReadPageCounter, this->ixWritePageCounter, ixAppendPageCounter);
//        this->ixWritePageCounter = writePageCount;
//        this->ixAppendPageCounter = appendPageCount;
//        this->ixReadPageCounter = readPageCount;
        readPageCount = this->ixReadPageCounter;
        writePageCount = this->ixWritePageCounter;
        appendPageCount = this->ixAppendPageCounter;
        return 0;
    }



} // namespace PeterDB