#include "src/include/pfm.h"
#include<iostream>
using namespace std;
namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        if (isFileExisting(fileName)) { return 1; } //file already exists
        FILE *f = fopen(fileName.c_str(), "w");
        if (f == nullptr) { return -1; } //Fail to create a new file.
        fclose(f);

        return 0;
    }

    bool PagedFileManager::isFileExisting(const std::string &fileName) const {
        FILE *f = fopen(fileName.c_str(), "r");
        if (f == nullptr) { return false; } // file does not exist
        fclose(f);

        return true;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if (!isFileExisting(fileName)) { return -1; }
        string name(fileName);
        return remove(fileName.c_str());  //Remove the file. 0: success; others: fail
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if(!isFileExisting(fileName)) {return -1;} //file does not exist
        if (fileHandle.file) { return -1; } //this fh has already been used;
        //PagedFileManager failed to open the file: this file does not exist.
        if (!(fileHandle.file = fopen(fileName.c_str(), "rb+"))) { return -1; }
        fileHandle.fileName = fileName;
        //rwa - root
        if(fileHandle.getNumberOfPages() > 0) {
            fseek(fileHandle.file, 0, SEEK_SET);
            fread(&fileHandle.readPageCounter, INT_FIELD_LEN, 1, fileHandle.file);
            fseek(fileHandle.file, INT_FIELD_LEN, SEEK_SET);
            fread(&fileHandle.writePageCounter, INT_FIELD_LEN, 1, fileHandle.file);
            fseek(fileHandle.file, 2 * INT_FIELD_LEN, SEEK_SET);
            fread(&fileHandle.appendPageCounter, INT_FIELD_LEN, 1, fileHandle.file);
            fileHandle.readPageCounter++;
//            fseek(fileHandle.file, 3 * INT_FIELD_LEN, SEEK_SET);
//            todo: remove
            fread(&fileHandle.root, INT_FIELD_LEN, 1, fileHandle.file);
        }
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (!fileHandle.file) { return -1; }      //Unused fh
        if (fileHandle.appendPageCounter != 0 || fileHandle.writePageCounter != 0 ||
            fileHandle.appendPageCounter != 0) {
            //r-w-a
            fileHandle.writePageCounter++;
            fseek(fileHandle.file, 0, SEEK_SET);
            fwrite(&fileHandle.readPageCounter, INT_FIELD_LEN, 1, fileHandle.file);
            fseek(fileHandle.file, INT_FIELD_LEN, SEEK_SET);
            fwrite(&fileHandle.writePageCounter, INT_FIELD_LEN, 1, fileHandle.file);
            fseek(fileHandle.file, 2 * INT_FIELD_LEN, SEEK_SET);
            fwrite(&fileHandle.appendPageCounter, INT_FIELD_LEN, 1, fileHandle.file);
            fseek(fileHandle.file, 3 * INT_FIELD_LEN, SEEK_SET);
//            todo: remove
            fwrite(&fileHandle.root, INT_FIELD_LEN, 1, fileHandle.file);
            fflush(fileHandle.file);
        }
        fclose(fileHandle.file);
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        root = 0;
        this->file = NULL;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (!this->file) { return -1; } //File does not exist.
        if (getNumberOfPages() <= pageNum) { return -1; } //error: exceed the max page #
//        if (getNumberOfPages() < pageNum) { return -1; } //error: exceed the max page #
        long startPosition = PAGE_SIZE * (pageNum + 1);    //page # starts from 0; the first page is the hidden page
        fseek(file, startPosition, SEEK_SET); //move the pointer to the required page
        fread(data, 1, PAGE_SIZE, file);
        this->readPageCounter++;
        return 0;
    }


    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (!this->file) { return -1; } //File does not exist.
        if (getNumberOfPages() <= pageNum) { return -1; } //error: exceed the max page #
        long startPosition = PAGE_SIZE * (pageNum + 1);    //page # starts from 0
        fseek(file, startPosition, SEEK_SET); //move the pointer to the required page
        fwrite(data, 1, PAGE_SIZE, file);
        this->writePageCounter++;
        fflush(this->file);
        return 0;
    }


    RC FileHandle::appendPage(const void *data) {
        if (getNumberOfPages() == 0) { //totally empty file, even without a hidden page
            char hiddenPageData[PAGE_SIZE];
            fseek(this->file, 0, SEEK_END);
            if (fwrite(hiddenPageData, PAGE_SIZE, 1, this->file) == 0) {
                return -1; //fail to add hidden page;
            }
            this->appendPageCounter++;
        }
        fseek(this->file, 0, SEEK_END);
        if (fwrite(data, 1, PAGE_SIZE, this->file) == 0) { return -1; } //fail to append;
        this->appendPageCounter++;
        fflush(file);
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        if (!this->file) { return -1; } //File does not exist.
        if (!file) { return -1; }
        fseek(file, 0, SEEK_END);
        int numOfPages = 0;
//        std::cout<<ftell(file)<<endl;
        if ((ftell(file) / PAGE_SIZE) - 1 > 0) { numOfPages = ftell(file) / PAGE_SIZE - 1; }
//        cout<<"---------------#of pgs= " <<numOfPages<<endl;
        return numOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = this->readPageCounter;
        writePageCount = this->writePageCounter;
        appendPageCount = this->appendPageCounter;
        return 0;
    }
} // namespace PeterDB