#include "src/include/pfm.h"

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
        FILE *file;
        if (file = fopen(fileName.c_str(), "r")) {
//        cout<<"Fail to create the file: already exists."<<endl;
            return -1;              //File already exists.
        }
        file = fopen(fileName.c_str(), "wb");

        if (file == NULL) {
            return -1;              //Fail to create a new file.
        }
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        FILE *file = fopen(fileName.c_str(), "r");
        if (file == NULL) {
            return -1;              //File does not exist.
        }
        fclose(file);
        return remove(fileName.c_str());  //Remove the file. 0: success; others: fail
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if (fileHandle.getFileName().length() != 0) {return -1; } //this fileHandle has already been used;

        FILE *file = fopen(fileName.c_str(), "rb+");
        if (file == NULL) {
            return -1;              //PagedFileManager failed to open the file: this file does not exist.
        }
        fileHandle.setFile(file);
        fileHandle.setFileName(fileName);
//    initiate the counters in the handle.
        fileHandle.appendPageCounter = 0;
        fileHandle.writePageCounter = 0;
        fileHandle.readPageCounter = 0;

        void *hiddenPageData = malloc(PAGE_SIZE);
        RC rc = fileHandle.readHiddenPage(hiddenPageData);
        if (rc == 0) {
            memcpy(&fileHandle.readPageCounter, (char *) hiddenPageData, INT_FIELD_LEN); //read counter
            memcpy(&fileHandle.appendPageCounter, (char *) hiddenPageData + INT_FIELD_LEN, INT_FIELD_LEN); //read counter
            memcpy(&fileHandle.writePageCounter, (char *) hiddenPageData + (2 * INT_FIELD_LEN), INT_FIELD_LEN); //read counter
//        int rCounter = 0, wCounter = 0, aCounter = 0;
//        memcpy(&rCounter, (char *) hiddenPageData, INT_FIELD_LEN); //read counter
//        memcpy(&wCounter, (char *) hiddenPageData + INT_FIELD_LEN, INT_FIELD_LEN); //read counter
//        memcpy(&aCounter, (char *) hiddenPageData + (2 * INT_FIELD_LEN), INT_FIELD_LEN); //read counter
//        free(hiddenPageData);
//        fileHandle.readPageCounter = rCounter;
//        fileHandle.appendPageCounter = aCounter;
//        fileHandle.writePageCounter = wCounter;
        }
        free(hiddenPageData);

        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (fileHandle.getFileName().length() <= 0 || fileHandle.getFile() == NULL) {
            return -1;      //Unused fileHandle
        }
        void *hiddenPageData = malloc(PAGE_SIZE);
        fileHandle.readHiddenPage(hiddenPageData);
//    R W A
        int rCounter = fileHandle.readPageCounter;
        int wCounter = fileHandle.writePageCounter;
        int aCounter = fileHandle.appendPageCounter;
        memcpy((char *) hiddenPageData, &rCounter, INT_FIELD_LEN);
        memcpy((char *) hiddenPageData + INT_FIELD_LEN, &wCounter, INT_FIELD_LEN);
        memcpy((char *) hiddenPageData + (2 * INT_FIELD_LEN), &aCounter, INT_FIELD_LEN);
        fileHandle.writeHiddenPage(hiddenPageData);
        free(hiddenPageData);
//    fclose(fileHandle.getFile());

        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (!isFileExisted()) {
            return -1; //File does not exist.
        }
        FILE *file = this->getFile();

        if (getNumberOfPages() <= pageNum) {
            return -1;  //error: exceed the max page #
        }
        long startPosition = PAGE_SIZE * (pageNum + 1);    //page # starts from 0; the first page is the hidden page
        fseek(file, startPosition, SEEK_SET); //move the pointer to the required page
        fread(data, 1, PAGE_SIZE, file);
        this->readPageCounter++;
        return 0;
    }


    RC FileHandle::readHiddenPage(void *data) {
        if (!isFileExisted()) {
//        cout<<"FileHandle failed to read the hidden page: file does not exist."<<endl;
            return -1; //File does not exist.
        }
        FILE *file = this->getFile();

        fseek(file, 0, SEEK_END);
        if ((ftell(file) / PAGE_SIZE) == 0) {
//        cout<<"FileHandle failed to read the hidden page: number of pages <= 0."<<endl;
            return -1;  //no data in file, so no hidden page.
        }
        fseek(file, 0, SEEK_SET); //move the pointer to the required page
        fread(data, 1, PAGE_SIZE, file);
        this->readPageCounter++;

        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (!isFileExisted()) {
            return -1; //File does not exist.
        }
        FILE *file = this->getFile();
        if (getNumberOfPages() <= pageNum) {
            return -1;  //error: exceed the max page #
        }
        long startPosition = PAGE_SIZE * (pageNum + 1);    //page # starts from 0
        fseek(file, startPosition, SEEK_SET); //move the pointer to the required page
        fwrite(data, 1, PAGE_SIZE, file);
        this->writePageCounter++;
        return 0;
    }


    RC FileHandle::writeHiddenPage(void *data) {
        if (!isFileExisted()) {
            return -1; //File does not exist.
        }
        FILE *file = this->getFile();
        this->writePageCounter++; //r-w-a
        memcpy((char *) data + sizeof(int), &writePageCounter, sizeof(int));
        fseek(file, 0, SEEK_SET); //move the pointer to the required page
        fwrite(data, 1, PAGE_SIZE, file);

        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        FILE *fp = this->getFile();
        size_t ret;
//    if(getNumberOfPages() == 0){ //totally empty file, even without a hidden page
        void *hiddenPageData = malloc(PAGE_SIZE);
        int res = readHiddenPage(hiddenPageData);///1. hidden page可能有root信息
        if (res != 0) {
            int initialCounterValue = 0;
            memcpy((char *) hiddenPageData, &initialCounterValue, INT_FIELD_LEN);
            memcpy((char *) hiddenPageData + INT_FIELD_LEN, &initialCounterValue, INT_FIELD_LEN);
            memcpy((char *) hiddenPageData + (2 * INT_FIELD_LEN), &initialCounterValue, INT_FIELD_LEN);
            fseek(fp, 0, SEEK_SET);
            ret = fwrite(hiddenPageData, 1, PAGE_SIZE, fp);
            if (ret == 0) {
                return -1; //fail to add hidden page;
            }
            this->appendPageCounter++;
        }
        free(hiddenPageData);
//    }
        fseek(fp, 0, SEEK_END);
        ret = fwrite(data, 1, PAGE_SIZE, fp);
        if (ret == 0) {
            return -1; //fail to append;
        }
        this->appendPageCounter++;
        fflush(fp);
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        if (!isFileExisted()) {
            return -1; //File does not exist.
        }
        FILE *file = this->getFile();
        if (file == NULL) {
            return -1;
        }
        fseek(file, 0, SEEK_END);
        int numOfPages = 0;
        if ((ftell(file) / PAGE_SIZE) - 1 > 0) {
            numOfPages = ftell(file) / PAGE_SIZE - 1;
        }
        return numOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        //    if (this->readPageCounter == 0 && this->appendPageCounter == 0 && this->writePageCounter == 0) {
//        //get the counters from the file;
//        void *hiddenPageData = malloc(PAGE_SIZE);
//        RC rc = this->readHiddenPage(hiddenPageData);
//        if (rc != 0) {
//            return -1;
//        }
//        int rCounter = 0, wCounter = 0, aCounter = 0;
//        memcpy(&rCounter, (char *) hiddenPageData, INT_FIELD_LEN); //read counter
//        memcpy(&wCounter, (char *) hiddenPageData + INT_FIELD_LEN, INT_FIELD_LEN); //read counter
//        memcpy(&aCounter, (char *) hiddenPageData + (2 * INT_FIELD_LEN), INT_FIELD_LEN); //read counter
//        free(hiddenPageData);
//        this->readPageCounter = rCounter;
//        this->appendPageCounter = aCounter;
//        this->writePageCounter = wCounter;
//    }
        readPageCount = this->readPageCounter;
        writePageCount = this->writePageCounter;
        appendPageCount = this->appendPageCounter;
        return 0;
    }


    bool FileHandle::isFileExisted() {
        if (this->getFile() == NULL || this->getFileName().length() <= 0) {
            return false;              //File does not exist.
        }
        return true;
    }

} // namespace PeterDB