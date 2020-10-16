## Project 1 Report


### 1. Basic information
 - Team #: 666
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-666
 - Student 1 UCI NetID: 56204511
 - Student 1 Name: Shanshan Han



### 2. Internal Record Format
- Show your record format design.

  1) Record Format
     - In each page, the records are stored one by one. Variable-length records are used, so that the waste of space is reduced as much as possible.
       Each slot only contains one record, and the length of slot is also variable.
     - Deal with different type of data:
        - For integers and floats, each value is simply stored as 4 bytes.
        - For varChars, each value contains 4 bytes for the length of the characters at the beginning(length information).
          When read a varChar, firstly we obtain the length information. Then, we substr the string according to
          the length information to obtain the actual characters.

  2) O(1) field access.
     - A slot table is set to record the offset and the length of records (slots) at the end of the page.
       The slot table begins with "$$". The field length of the slot table is set to SLOT_TABLE_FIELD_LEN.
       In the slot table, the first 2 * SLOT_TABLE_FIELD_LEN bytes are for the first record, and the second
       2 * SLOT_TABLE_FIELD_LEN are for the second record, and so on. SLOT_TABLE_FIELD_LEN is 4, equal to sizeof(int).
     - The system use the rid (page number + slot number) and the slot table to access records in O(1) time.
       When reading a record, according to the page number, the fileHandle reads the page data. Then, according to
       the slot number, the system refers to the slot table and obtains the offset and the length of the record.
       According to the offset and the length, the pointer is moved to (char*)pageData + offset, which is the beginning
       of the record; then, characters are read according to the length, and these characters are the contents of the record.

  3) Varchar field: suppose the length of the varchar is len, then the required space for the varchar is sizeof(Int) + len.
     At the beginning of the varchar, 4 bytes are used to record the length of the varchar. When reading a varchar field,
     the beginning 4 bytes are used to decide the length of the varchar.



### 3. Page Format
- Show your page format design.

  1) PagedFileManager class: handle the file creation, deletion, opening, and closing.
      - RC createFile (const string &fileName): create an empty file with a name of fileName.
      - RC destroyFile (const string &fileName): destroys the paged file whose name is fileName.
      - RC openFile (const string &fileName, FileHandle &fileHandle): open the paged file with a name of fileName, and pass
            the fileHandle for the current file.
      - RC closeFile (FileHandle &fileHandle): close the open file instance referred to by fileHandle.

  2) FileHandle Class: FileHandles are used to manipulate the pages of the file. A pointer *file is set to record which
        file this fileHandle is manipulating.
      - RC readPage(PageNum pageNum, void *data): read the page into *data.
      - RC writePage(PageNum pageNum, const void *data): write the given data into a page specified by pageNum.
      - RC appendPage(const void *data): append a new page to the end of the file and write the given data into the new page.
      - unsigned getNumberOfPages(): return the total number of pages currently in the file, calculated by "file size / page size",
            where the page size is set to 4K.
      - RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount): return the current
            counter values of this FileHandle in the three given variables.

- Explain your slot directory design if applicable.
slot directory: "$$ |offset  length | offset  length | offset  length | offset  length ..."


### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.
Traverse each page. If the free space in the current page > length of the record, the record is inserted into this page. Then go to the
 slot directory. If there is an empty slot, the slot info will be stored at the empty slot; otherwise, the directory
 will be appended a new slot.




- How many hidden pages are utilized in your design?
1


- Show your hidden page(s) format design if applicable
readPageCounter(4 bytes) writePageCount(4 bytes) appendPageCount(4 bytes)


### 5. Implementation Detail
- Other implementation details goes here.

    1) This system use RecordBasedFileManager to handle record-based operations such as inserting, updating, deleting, and reading records.
      - RC createFile(const string &fileName): create a record-based file called fileName.
      - RC destroyFile(const string &fileName): destroy the record-based file whose name is fileName.
      - RC openFile(const string &fileName, FileHandle &fileHandle): open the record-based file whose name is fileName.
      - RC closeFile(FileHandle &fileHandle): closeS the open file instance referred to by fileHandle.
      - RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid):insert
            a new record into the file according to the record descriptor. When inserting a record, firstly whether the
            current page has enough space is checked. If the current page is enough for the coming record, this record is
            inserted into this page; otherwise, the system will find the page with enough space for this record.
      - RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data): read
            the record by rid.
      - RC printRecord(const vector<Attribute> &recordDescriptor, const void *data): print the content of the record (*data)
            according to the recordDescriptor.

    2) Deal with NULL field: For each record, it contains 2 parts: the null indicator, and the record data. The null indicator
        passes the null information of the record data. The length n is calculated as ceil(number of fields / 8). If the
        value for a field is NULL, the corresponding bit in the null indicator is set to 1. And the record data only
        contains not-null values.



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)