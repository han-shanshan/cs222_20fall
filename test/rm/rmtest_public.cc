#include "test/utils/rm_test_util.h"

namespace PeterDBTesting {
    TEST_F(RM_Catalog_Test, create_and_delete_tables) {

        // Try to delete the System Catalog.
        // If this is the first time, it will generate an error. It's OK and we will ignore that.
        rm.deleteCatalog();

        std::string tableName = "should_not_be_created";
        // Delete the actual file
        remove(tableName.c_str());

        // Create a table should not succeed without Catalog
        std::vector<PeterDB::Attribute> table_attrs = parseDDL(
                "CREATE TABLE " + tableName + " (field1 INT, field2 REAL, field3 VARCHAR(20), field4 VARCHAR(90))");
        ASSERT_NE(rm.createTable(tableName, table_attrs), success)
                                    << "Create table " << tableName << " should not succeed.";
        ASSERT_FALSE(fileExists(tableName)) << "Table " << tableName << " file should not exist now.";

        // Create Catalog
        ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";

        for (int i = 1; i < 5; i++) {
            tableName = "rm_test_table_" + std::to_string(i);

            table_attrs = parseDDL(
                    "CREATE TABLE " + tableName + " (emp_name VARCHAR(40), age INT, height REAL, salary REAL))");
            // Delete the actual file
            remove(tableName.c_str());
            // Create a table
            ASSERT_EQ(rm.createTable(tableName, table_attrs), success)
                                        << "Create table " << tableName << " should succeed.";

            ASSERT_TRUE(fileExists(tableName)) << "Table " << tableName << " file should exist now.";

        }

        for (int i = 1; i < 5; i++) {
            tableName = "rm_test_table_" + std::to_string(i);
            // Delete the table
            ASSERT_EQ(rm.deleteTable(tableName), success) << "Delete table " << tableName << " should succeed.";
            ASSERT_FALSE(fileExists(tableName)) << "Table " << tableName << " file should not exist now.";
        }

        // Delete the non-existence table
        tableName = "non_existence_table";
        ASSERT_NE(rm.deleteTable(tableName), success)
                                    << "Delete non-existence table " << tableName << " should not succeed.";
        ASSERT_FALSE(fileExists(tableName)) << "Table " << tableName << " file should not exist now.";

        // Delete Catalog
        ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";

    }

    TEST_F(RM_Tuple_Test, get_attributes) {
        // Functions Tested
        // 1. getAttributes

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        ASSERT_EQ(attrs[0].name, "emp_name") << "Attribute is not correct.";
        ASSERT_EQ(attrs[0].type, PeterDB::TypeVarChar) << "Attribute is not correct.";
        ASSERT_EQ(attrs[0].length, 50) << "Attribute is not correct.";
        ASSERT_EQ(attrs[1].name, "age") << "Attribute is not correct.";
        ASSERT_EQ(attrs[1].type, PeterDB::TypeInt) << "Attribute is not correct.";
        ASSERT_EQ(attrs[1].length, 4) << "Attribute is not correct.";
        ASSERT_EQ(attrs[2].name, "height") << "Attribute is not correct.";
        ASSERT_EQ(attrs[2].type, PeterDB::TypeReal) << "Attribute is not correct.";
        ASSERT_EQ(attrs[2].length, 4) << "Attribute is not correct.";
        ASSERT_EQ(attrs[3].name, "salary") << "Attribute is not correct.";
        ASSERT_EQ(attrs[3].type, PeterDB::TypeReal) << "Attribute is not correct.";
        ASSERT_EQ(attrs[3].length, 4) << "Attribute is not correct.";

    }

    TEST_F(RM_Tuple_Test, insert_and_read_tuple) {
        // Functions tested
        // 1. Insert Tuple
        // 2. Read Tuple


        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert a tuple into a table
        std::string name = "Peter Anteater";
        size_t nameLength = name.length();
        unsigned age = 27;
        float height = 169.2;
        float salary = 9999.99;
        prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);

        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";


        // Given the rid, read the tuple from table
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success)
                                    << "RelationManager::readTuple() should succeed.";

        std::ostringstream stream;
        ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success) << "Print tuple should succeed.";

        checkPrintRecord("emp_name: Peter Anteater, age: 27, height: 169.2, salary: 9999.99",
                         stream.str());



        // Check the returned tuple
        ASSERT_EQ(memcmp(inBuffer, outBuffer, tupleSize), 0) << "The returned tuple is not the same as the inserted.";
    }

    TEST_F(RM_Tuple_Test, insert_and_delete_and_read_tuple) {
        // Functions Tested
        // 1. Insert tuple
        // 2. Delete Tuple **
        // 3. Read Tuple

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert the Tuple
        std::string name = "Peter";
        size_t nameLength = name.length();
        unsigned age = 18;
        float height = 157.8;
        float salary = 890.2;
        prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);

        std::ostringstream stream;
        ASSERT_EQ(rm.printTuple(attrs, inBuffer, stream), success) << "Print tuple should succeed.";
        checkPrintRecord("emp_name: Peter, age: 18, height: 157.8, salary: 890.2",
                         stream.str());

        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Delete the tuple
        ASSERT_EQ(rm.deleteTuple(tableName, rid), success)
                                    << "RelationManager::deleteTuple() should succeed.";

        // Read Tuple after deleting it - should not succeed
        memset(outBuffer, 0, 200);
        ASSERT_NE(rm.readTuple(tableName, rid, outBuffer), success)
                                    << "Reading a deleted tuple should not succeed.";

        // Check the returned tuple
        ASSERT_NE(memcmp(inBuffer, outBuffer, tupleSize), 0) << "The returned tuple should not match the inserted.";

    }

    TEST_F(RM_Tuple_Test, insert_and_update_and_read_tuple) {
        // Functions Tested
        // 1. Insert Tuple
        // 2. Update Tuple
        // 3. Read Tuple

        size_t tupleSize = 0;
        size_t updatedTupleSize = 0;
        PeterDB::RID updatedRID;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);


        // Test Insert the Tuple
        std::string name = "Paul";
        size_t nameLength = name.length();
        unsigned age = 28;
        float height = 164.7;
        float salary = 7192.8;
        prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        std::ostringstream stream;
        ASSERT_EQ(rm.printTuple(attrs, inBuffer, stream), success) << "Print tuple should succeed.";
        checkPrintRecord("emp_name: Paul, age: 28, height: 164.7, salary: 7192.8",
                         stream.str());

        // Test Update Tuple
        memset(inBuffer, 0, 200);
        prepareTuple(attrs.size(), nullsIndicator, 7, "Barbara", age, height, 12000, inBuffer, updatedTupleSize);
        ASSERT_EQ(rm.updateTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::updateTuple() should succeed.";

        // Test Read Tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success)
                                    << "RelationManager::readTuple() should succeed.";

        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(rm.printTuple(attrs, inBuffer, stream), success) << "Print tuple should succeed.";
        checkPrintRecord("emp_name: Barbara, age: 28, height: 164.7, salary: 12000",
                         stream.str());

        // Check the returned tuple
        ASSERT_EQ(memcmp(inBuffer, outBuffer, updatedTupleSize), 0)
                                    << "The returned tuple is not the same as the updated.";

    }

    TEST_F(RM_Tuple_Test, read_attribute) {
        // Functions Tested
        // 1. Insert tuples
        // 2. Read Attribute

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);


        // Test Insert the Tuple
        std::string name = "Paul";
        size_t nameLength = name.length();
        unsigned age = 57;
        float height = 165.5;
        float salary = 480000;
        prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Test Read Attribute
        ASSERT_EQ(rm.readAttribute(tableName, rid, "salary", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        float returnedSalary = *(float *) ((uint8_t *) outBuffer + 1);
        ASSERT_EQ(salary, returnedSalary) << "The returned salary does not match the inserted.";

        // Test Read Attribute
        memset(outBuffer, 0, 200);
        ASSERT_EQ(rm.readAttribute(tableName, rid, "age", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        unsigned returnedAge = *(unsigned *) ((uint8_t *) outBuffer + 1);
        ASSERT_EQ(age, returnedAge) << "The returned age does not match the inserted.";

    }

    TEST_F(RM_Tuple_Test, delete_table) {
        // Functions Tested
        // 0. Insert tuple;
        // 1. Read Tuple
        // 2. Delete Table
        // 3. Read Tuple
        // 4. Insert Tuple

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Test Insert the Tuple
        std::string name = "Paul";
        size_t nameLength = name.length();
        unsigned age = 28;
        float height = 165.5;
        float salary = 7000;
        prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";


        // Test Read Tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success)
                                    << "RelationManager::readTuple() should succeed.";

        // Test Delete Table
        ASSERT_EQ(rm.deleteTable(tableName), success)
                                    << "RelationManager::deleteTable() should succeed.";

        // Reading a tuple on a deleted table
        memset(outBuffer, 0, 200);
        ASSERT_NE(rm.readTuple(tableName, rid, outBuffer), success)
                                    << "RelationManager::readTuple() on a deleted table should not succeed.";

        // Inserting a tuple on a deleted table
        ASSERT_NE(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() on a deleted table should not succeed.";

        // Check the returned tuple
        ASSERT_NE(memcmp(inBuffer, outBuffer, tupleSize), 0) << "The returned tuple should not match the inserted.";

        destroyFile = false; // the table is already deleted.

    }

    TEST_F(RM_Scan_Test, simple_scan) {
        // Functions Tested
        // 1. Simple scan

        int numTuples = 100;
        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Test Insert Tuple
        PeterDB::RID rids[numTuples];
        std::set<unsigned> ages;
        for (int i = 0; i < numTuples; i++) {
            // Insert Tuple
            auto height = (float) i;
            unsigned age = 20 + i;
            prepareTuple(attrs.size(), nullsIndicator, 6, "Tester", age, height, age * 12.5, inBuffer, tupleSize);
            ages.insert(age);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";

            rids[i] = rid;
            memset(inBuffer, 0, 200);
        }
        cout<<"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"<<endl;

        // Set up the iterator
        std::vector<std::string> attributes{"age"};

        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, NULL, attributes, rmsi), success)
                                    << "RelationManager::scan() should succeed.";
        cout<<"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"<<endl;

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            unsigned returnedAge = *(unsigned *) ((uint8_t *) outBuffer + 1);
            auto target = ages.find(returnedAge);
            ASSERT_NE(target, ages.end()) << "Returned age is not from the inserted ones.";
            ages.erase(target);
        }
        cout<<"cccccccccccccccccccccccccccccccccccccccccccccccc"<<endl;
    }

    TEST_F(RM_Scan_Test, simple_scan_after_table_deletion) {
        // Functions Tested
        // 1. Simple scan
        // 2. Delete the given table
        // 3. Simple scan

        int numTuples = 65536;
        outBuffer = malloc(200);

        std::set<int> ages;

        for (int i = 0; i < numTuples; i++) {
            int age = 50 - i;
            ages.insert(age);
        }

        // Set up the iterator
        std::vector<std::string> attributes{"Age"};

        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, NULL, attributes, rmsi), success)
                                    << "RelationManager::scan() should succeed.";
        cout<<"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"<<endl;

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            unsigned returnedAge = *(unsigned *) ((uint8_t *) outBuffer + 1);
            auto target = ages.find(returnedAge);
            ASSERT_NE(target, ages.end()) << "Returned age is not from the inserted ones.";
            ages.erase(target);
        }
        cout<<"cccccccccccccccccccccccccccccccccccccccccccc"<<endl;

        // Close the iterator
        rmsi.close();

        // Delete a Table
        ASSERT_EQ(rm.deleteTable(tableName), success) << "RelationManager::deleteTable() should succeed.";

        // Scan on a deleted table
        ASSERT_NE(rm.scan(tableName, "", PeterDB::NO_OP, NULL, attributes, rmsi), success)
                                    << "RelationManager::scan() should not succeed on a deleted table.";

        destroyFile = false; // the table is already deleted.
        cout<<"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"<<endl;

    }

    TEST_F(RM_Large_Table_Test, insert_large_tuples) {
        // Functions Tested for large tables:
        // 1. getAttributes
        // 2. insert tuple

        // Remove the leftover files from previous runs
        remove("rid_files");
        remove("size_files");
        remove(tableName.c_str());

        // Try to delete the System Catalog.
        // If this is the first time, it will generate an error. It's OK and we will ignore that.
        rm.deleteCatalog();

        // Create Catalog
        ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";
        createLargeTable(tableName);

        inBuffer = malloc(bufSize);
        int numTuples = 5000;

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert numTuples tuples into table
        for (int i = 0; i < numTuples; i++) {
            // Test insert Tuple
            size_t size = 0;
            memset(inBuffer, 0, bufSize);
            prepareLargeTuple(attrs.size(), nullsIndicator, i, inBuffer, size);

            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            rids.emplace_back(rid);
            sizes.emplace_back(size);
        }

        writeRIDsToDisk(rids);
        writeSizesToDisk(sizes);

    }

    TEST_F(RM_Large_Table_Test, read_large_tuples) {
        // This test is expected to be run after RM_Large_Table_Test::insert_large_tuples

        // Functions Tested for large tables:
        // 1. read tuple

        size_t size = 0;
        int numTuples = 5000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        // read the saved rids and the sizes of records
        readRIDsFromDisk(rids, numTuples);
        readSizesFromDisk(sizes, numTuples);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);
            memset(outBuffer, 0, bufSize);

            ASSERT_EQ(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should succeed.";

            size = 0;
            prepareLargeTuple(attrs.size(), nullsIndicator, i, inBuffer, size);
            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, size), 0) << "the read tuple should match the inserted tuple";

        }

    }

    TEST_F(RM_Large_Table_Test, update_and_read_large_tuples) {
        // This test is expected to be run after RM_Large_Table_Test::insert_large_tuples

        // Functions Tested for large tables:
        // 1. update tuple
        // 2. read tuple

        int numTuples = 5000;
        unsigned numTuplesToUpdate1 = 2000;
        unsigned numTuplesToUpdate2 = 2000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        readRIDsFromDisk(rids, numTuples);
        readSizesFromDisk(sizes, numTuples);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Update the first numTuplesToUpdate1 tuples
        size_t size = 0;
        for (int i = 0; i < numTuplesToUpdate1; i++) {
            memset(inBuffer, 0, bufSize);
            rid = rids[i];

            prepareLargeTuple(attrs.size(), nullsIndicator, i + 10, inBuffer, size);

            ASSERT_EQ(rm.updateTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::updateTuple() should succeed.";

            sizes[i] = size;
            rids[i] = rid;
        }

        // Update the last numTuplesToUpdate2 tuples
        for (unsigned i = numTuples - numTuplesToUpdate2; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);
            rid = rids[i];

            prepareLargeTuple(attrs.size(), nullsIndicator, i - 10, inBuffer, size);

            ASSERT_EQ(rm.updateTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::updateTuple() should succeed.";

            sizes[i] = size;
            rids[i] = rid;
        }

        // Read the updated records and check the integrity
        for (unsigned i = 0; i < numTuplesToUpdate1; i++) {
            memset(inBuffer, 0, bufSize);
            memset(outBuffer, 0, bufSize);
            prepareLargeTuple(attrs.size(), nullsIndicator, i + 10, inBuffer, size);

            ASSERT_EQ(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should succeed.";

            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, size), 0) << "the read tuple should match the updated tuple";

        }

        for (unsigned i = numTuples - numTuplesToUpdate2; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);
            memset(outBuffer, 0, bufSize);
            prepareLargeTuple(attrs.size(), nullsIndicator, i - 10, inBuffer, size);

            ASSERT_EQ(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should succeed.";

            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, size), 0) << "the read tuple should match the updated tuple";

        }

        // Read the non-updated records and check the integrity
        for (unsigned i = numTuplesToUpdate1; i < numTuples - numTuplesToUpdate2; i++) {
            memset(inBuffer, 0, bufSize);
            memset(outBuffer, 0, bufSize);
            prepareLargeTuple(attrs.size(), nullsIndicator, i, inBuffer, size);

            ASSERT_EQ(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should succeed.";

            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, size), 0) << "the read tuple should match the inserted tuple";

        }

    }

    TEST_F(RM_Large_Table_Test, delete_and_read_large_tuples) {
        // This test is expected to be run after RM_Large_Table_Test::insert_large_tuples

        // Functions Tested for large tables:
        // 1. delete tuple
        // 2. read tuple

        unsigned numTuples = 5000;
        unsigned numTuplesToDelete = 2000;
        outBuffer = malloc(bufSize);

        readRIDsFromDisk(rids, numTuples);

        // Delete the first numTuplesToDelete tuples
        for (unsigned i = 0; i < numTuplesToDelete; i++) {

            ASSERT_EQ(rm.deleteTuple(tableName, rids[i]), success) << "RelationManager::deleteTuple() should succeed.";
        }

        // Try to read the first numTuplesToDelete deleted tuples
        for (unsigned i = 0; i < numTuplesToDelete; i++) {

            ASSERT_NE(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() on a deleted tuple should not succeed.";

        }

        // Read the non-deleted tuples
        for (unsigned i = numTuplesToDelete; i < numTuples; i++) {
            ASSERT_EQ(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should succeed.";

        }

    }

    TEST_F(RM_Large_Table_Test, scan_large_tuples) {

        // Functions Tested for large tables
        // 1. scan

        destroyFile = true;   // To clean up after test.

        std::vector<std::string> attrs{
                "attr29", "attr15", "attr25"
        };

        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, NULL, attrs, rmsi), success) <<
                                                                                      "RelationManager::scan() should succeed.";

        unsigned count = 0;
        outBuffer = malloc(bufSize);

        size_t nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {

            size_t offset = 0;

            float attr29 = *(float *) ((uint8_t *) outBuffer + nullAttributesIndicatorActualSize);
            offset += 4;

            unsigned size = *(unsigned *) ((uint8_t *) outBuffer + offset + nullAttributesIndicatorActualSize);
            offset += 4;

            auto *attr15 = (uint8_t *) malloc(size + 1);
            memcpy(attr15, (uint8_t *) outBuffer + offset + nullAttributesIndicatorActualSize, size);
            attr15[size] = 0;
            offset += size;
            char target;
            for (size_t k = 0; k < size; k++) {
                if (k == 0) {
                    target = attr15[k];
                } else {
                    ASSERT_EQ(target, attr15[k]) << "Scanned VARCHAR has incorrect value";
                }
            }
            unsigned attr25 = *(unsigned *) ((uint8_t *) outBuffer + offset + nullAttributesIndicatorActualSize);

            ASSERT_EQ(attr29, attr25 + 1);
            free(attr15);
            count++;
            memset(outBuffer, 0, bufSize);
        }

        ASSERT_EQ(count, 3000) << "Number of scanned tuples is incorrect.";

    }

    TEST_F(RM_Scan_Test, conditional_scan) {
        // Functions Tested:
        // 1. Conditional scan

        bufSize = 100;
        size_t tupleSize = 0;
        unsigned numTuples = 1500;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        unsigned ageVal = 25;
        unsigned age;

        PeterDB::RID rids[numTuples];
        std::vector<uint8_t *> tuples;

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            auto height = (float) i;

            age = (rand() % 10) + 23;

            prepareTuple(attrs.size(), nullsIndicator, 6, "Tester", age, height, 123, inBuffer, tupleSize);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";

            rids[i] = rid;
        }

        // Set up the iterator
        std::string attr = "age";
        std::vector<std::string> attributes{attr};

        ASSERT_EQ(rm.scan(tableName, attr, PeterDB::GT_OP, &ageVal, attributes, rmsi), success)
                                    << "RelationManager::scan() should succeed.";

        memset(outBuffer, 0, bufSize);
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            age = *(unsigned *) ((uint8_t *) outBuffer + 1);
            ASSERT_GT(age, ageVal) << "Returned value from a scan is not correct.";
            memset(outBuffer, 0, bufSize);
        }
    }

    TEST_F(RM_Scan_Test, conditional_scan_with_null) {
        // Functions Tested:
        // 1. Conditional scan - including NULL values

        bufSize = 200;
        size_t tupleSize = 0;
        unsigned numTuples = 1500;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        unsigned ageVal = 25;
        unsigned age;

        PeterDB::RID rids[numTuples];
        std::vector<uint8_t *> tuples;
        std::string tupleName;

        bool nullBit;

        // GetAttributes
        std::vector<PeterDB::Attribute> attrs;
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize two NULL field indicators
        nullsIndicator = initializeNullFieldsIndicator(attrs);
        nullsIndicatorWithNull = initializeNullFieldsIndicator(attrs);

        // age field : NULL
        nullsIndicatorWithNull[0] = 64; // 01000000

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            auto height = (float) i;

            age = (rand() % 20) + 15;

            std::string suffix = std::to_string(i);

            if (i % 10 == 0) {
                tupleName = "TesterNull" + suffix;
                prepareTuple(attrs.size(), nullsIndicatorWithNull, tupleName.length(), tupleName, 0, height, 456,
                             inBuffer,
                             tupleSize);
            } else {
                tupleName = "Tester" + suffix;
                prepareTuple(attrs.size(), nullsIndicator, tupleName.length(), tupleName, age, height, 123, inBuffer,
                             tupleSize);
            }
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";

            rids[i] = rid;

        }

        // Set up the iterator
        std::string attr = "age";
        std::vector<std::string> attributes{attr};
        ASSERT_EQ(rm.scan(tableName, attr, PeterDB::GT_OP, &ageVal, attributes, rmsi), success)
                                    << "RelationManager::scan() should succeed.";

        memset(outBuffer, 0, bufSize);
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            // Check the first bit of the returned data since we only return one attribute in this test case
            // However, the age with NULL should not be returned since the condition NULL > 25 can't hold.
            // All comparison operations with NULL should return FALSE
            // (e.g., NULL > 25, NULL >= 25, NULL <= 25, NULL < 25, NULL == 25, NULL != 25: ALL FALSE)
            nullBit = *(bool *) ((uint8_t *) outBuffer) & ((unsigned) 1 << (unsigned) 7);
            ASSERT_FALSE(nullBit) << "NULL value should not be returned from a scan.";

            age = *(unsigned *) ((uint8_t *) outBuffer + 1);
            ASSERT_GT(age, ageVal) << "Returned value from a scan is not correct.";
            memset(outBuffer, 0, bufSize);

        }

    }

    TEST_F(RM_Catalog_Scan_Test, catalog_tables_table_check) {
        // Functions Tested:
        // 1. System Catalog Implementation - Tables table

        // Get Catalog Attributes
        ASSERT_EQ(rm.getAttributes("Tables", attrs), success) << "RelationManager::getAttributes() should succeed.";


        // There should be at least three attributes: table-id, table-name, file-name
        ASSERT_GE(attrs.size(), 3) << "Tables table should have at least 3 attributes.";
        ASSERT_FALSE(attrs[0].name != "table-id" || attrs[1].name != "table-name" || attrs[2].name != "file-name")
                                    << "Tables table's schema is not correct.";

        PeterDB::RID rid;
        bufSize = 1000;
        outBuffer = malloc(bufSize);

        // Set up the iterator
        std::vector<std::string> projected_attrs;
        projected_attrs.reserve(attrs.size());
        for (PeterDB::Attribute &attr : attrs) {
            projected_attrs.push_back(attr.name);
        }

        ASSERT_EQ(rm.scan("Tables", "", PeterDB::NO_OP, NULL, projected_attrs, rmsi), success)
                                    << "RelationManager::scan() should succeed.";

        int count = 0;

        // Check Tables table
        checkCatalog("table-id: x, table-name: Tables, file-name: Tables");

        // Check Columns table
        checkCatalog("table-id: x, table-name: Columns, file-name: Columns");

        // Keep scanning the remaining records
        memset(outBuffer, 0, bufSize);
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            count++;
            memset(outBuffer, 0, bufSize);
        }

        // There should be at least one more table
        ASSERT_GE(count, 1) << "There should be at least one more table.";

        // Deleting the catalog should fail.
        ASSERT_NE(rm.deleteTable("Tables"),
                  success && "RelationManager::deleteTable() on the system catalog table should not succeed.");

    }

    TEST_F(RM_Catalog_Scan_Test, catalog_columns_table_check) {

        // Functions Tested:
        // 1. System Catalog Implementation - Columns table

        // Get Catalog Attributes
        ASSERT_EQ(rm.getAttributes("Columns", attrs), success)
                                    << "RelationManager::getAttributes() should succeed.";

        // There should be at least five attributes: table-id, column-name, column-type, column-length, column-position
        ASSERT_GE(attrs.size(), 5) << "Columns table should have at least 5 attributes.";
        ASSERT_FALSE(attrs[0].name != "table-id" || attrs[1].name != "column-name" ||
                     attrs[2].name != "column-type" || attrs[3].name != "column-length" ||
                     attrs[4].name != "column-position") << "Columns table's schema is not correct.";

        bufSize = 1000;
        outBuffer = malloc(bufSize);

        // Set up the iterator
        std::vector<std::string> projected_attrs;
        for (const PeterDB::Attribute &attr : attrs) {
            projected_attrs.push_back(attr.name);
        }

        ASSERT_EQ(rm.scan("Columns", "", PeterDB::NO_OP, NULL, projected_attrs, rmsi), success)
                                    << "RelationManager::scan() should succeed.";

        // Check Tables table
        checkCatalog("table-id: x, column-name: table-id, column-type: 0, column-length: 4, column-position: 1");
        checkCatalog("table-id: x, column-name: table-name, column-type: 2, column-length: 50, column-position: 2");
        checkCatalog("table-id: x, column-name: file-name, column-type: 2, column-length: 50, column-position: 3");

        // Check Columns table
        checkCatalog("table-id: x, column-name: table-id, column-type: 0, column-length: 4, column-position: 1");
        checkCatalog(
                "table-id: x, column-name: column-name, column-type: 2, column-length: 50, column-position: 2");
        checkCatalog("table-id: x, column-name: column-type, column-type: 0, column-length: 4, column-position: 3");
        checkCatalog(
                "table-id: x, column-name: column-length, column-type: 0, column-length: 4, column-position: 4");
        checkCatalog(
                "table-id: x, column-name: column-position, column-type: 0, column-length: 4, column-position: 5");


        // Keep scanning the remaining records
        unsigned count = 0;
        memset(outBuffer, 0, bufSize);
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            count++;
            memset(outBuffer, 0, bufSize);
        }

        // There should be at least 4 more records for created table
        ASSERT_GE(count, 4) << "at least 4 more records for " << tableName;

        // Deleting the catalog should fail.
        ASSERT_NE(rm.deleteTable("Columns"),
                  success && "RelationManager::deleteTable() on the system catalog table should not succeed.");

    }

    TEST_F(RM_Version_Test, read_after_drop_attribute) {
        // Extra Credit Test Case - Functions Tested:
        // 1. Insert tuple
        // 2. Read Attributes
        // 3. Drop Attributes

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize two NULL field indicators
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert Tuple
        std::string name = "Peter Anteater";
        size_t nameLength = name.length();
        unsigned age = 24;
        float height = 185;
        float salary = 23333.3;
        prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Read Attribute
        ASSERT_EQ(rm.readAttribute(tableName, rid, "height", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        ASSERT_FLOAT_EQ(*(float *) ((uint8_t *) outBuffer + 1), height)
                                    << "Returned height does not match the inserted.";

        // Drop the attribute
        ASSERT_EQ(rm.dropAttribute(tableName, "height"), success) << "RelationManager::dropAttribute() should succeed.";

        // Read Tuple and print the tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        // Get the attribute from the table again
        std::vector<PeterDB::Attribute> attrs2;
        ASSERT_EQ(rm.getAttributes(tableName, attrs2), success) << "RelationManager::getAttributes() should succeed.";

        // The size of the original attribute vector size should be greater than the current one.
        ASSERT_GT(attrs.size(), attrs2.size()) << "attributes should be less than the previous version.";

        std::stringstream stream;
        ASSERT_EQ(rm.printTuple(attrs2, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";
        checkPrintRecord("emp_name: Peter Anteater, age: 24, salary: 23333.3", stream.str());
    }

    TEST_F(RM_Version_Test, read_after_add_attribute) {
        // Extra Credit Test Case - Functions Tested:
        // 1. Insert tuple
        // 2. Read Attributes
        // 3. Drop Attributes

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Test Add Attribute
        PeterDB::Attribute attr{
                "SSN", PeterDB::TypeInt, 4
        };
        ASSERT_EQ(rm.addAttribute(tableName, attr), success) << "RelationManager::addAttribute() should succeed.";


        // GetAttributes again
        std::vector<PeterDB::Attribute> attrs2;
        ASSERT_EQ(rm.getAttributes(tableName, attrs2), success) << "RelationManager::getAttributes() should succeed.";

        // The size of the original attribute vector size should be less than the current one.
        ASSERT_GT(attrs2.size(), attrs.size()) << "attributes should be more than the previous version.";

        // Initialize two NULL field indicators
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert Tuple
        std::string name = "Peter Anteater";
        size_t nameLength = name.length();
        unsigned age = 34;
        float height = 175.3;
        float salary = 24123.90;
        unsigned ssn = 123479765;

        prepareTupleAfterAdd(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, ssn, inBuffer,
                             tupleSize);

        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Read Tuple and print the tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        std::stringstream stream;
        ASSERT_EQ(rm.printTuple(attrs2, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";

        checkPrintRecord("emp_name: Peter Anteater, age: 34, height: 175.3, salary: 24123.90, ssn: 123479765",
                         stream.str());
    }

} // namespace PeterDBTesting