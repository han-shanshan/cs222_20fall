#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096
#define INT_FIELD_LEN 4
#include <string>
#include <climits>
#include <memory>
#include <cmath>
#include <cstdio>
#include <vector>
#include <cstdlib>
#include <cstring>

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        char b[PAGE_SIZE];

        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file

        bool isFileExisting(const std::string &fileName) const;

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        FILE *file;

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables
    };

} // namespace PeterDB

#endif // _pfm_h_