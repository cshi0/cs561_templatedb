#include <string>
#include <iostream>
#include <vector>

#include "gtest/gtest.h"
#include "templatedb/db.hpp"

// TEST(StorageTest, Tiering)
// {
//     std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db2"; 
//     templatedb::DB db;
//     db.open(fname, templatedb::TIERING);
//     std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_100000000_4.data";
//     db.load_data_file(data);
//     db.close();
// }

// TEST(StorageTest, Leveling)
// {
//     std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db2"; 
//     templatedb::DB db;
//     db.open(fname, templatedb::LEVELING);
//     std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_100000000_4.data";
//     db.load_data_file(data);
//     db.close();
// }

// TEST(StorageTest, Tiering)
// {
//     std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db_ratio6"; 
//     templatedb::DB db;
//     db.open(fname, templatedb::TIERING);
//     std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_5000000_4.data";
//     db.load_data_file(data);
//     db.close();
// }

// TEST(StorageTest, Leveling)
// {
//     std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db_ratio6"; 
//     templatedb::DB db;
//     db.open(fname, templatedb::LEVELING);
//     std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_5000000_4.data";
//     db.load_data_file(data);
//     db.close();
// }

TEST(StorageTest, Tiering100)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db_100"; 
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_1000000_4.data";
    db.load_data_file(data);
    db.close();
}

TEST(StorageTest, Leveling100)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db_100"; 
    templatedb::DB db;
    db.open(fname, templatedb::LEVELING);
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_1000000_4.data";
    db.load_data_file(data);
    db.close();
}

TEST(StorageTest, Tiering500)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db_500"; 
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_5000000_4.data";
    db.load_data_file(data);
    db.close();
}

TEST(StorageTest, Leveling500)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db_500"; 
    templatedb::DB db;
    db.open(fname, templatedb::LEVELING);
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_5000000_4.data";
    db.load_data_file(data);
    db.close();
}

TEST(StorageTest, Tiering1000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db_1000"; 
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_5000000_4.data";
    db.load_data_file(data);
    db.close();
}

TEST(StorageTest, Leveling1000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db_1000"; 
    templatedb::DB db;
    db.open(fname, templatedb::LEVELING);
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_10000000_4.data";
    db.load_data_file(data);
    db.close();
}

TEST(StorageTest, Tiering2000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db_2000"; 
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_20000000_4.data";
    db.load_data_file(data);
    db.close();
}

TEST(StorageTest, Leveling2000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db_2000"; 
    templatedb::DB db;
    db.open(fname, templatedb::LEVELING);
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_20000000_4.data";
    db.load_data_file(data);
    db.close();
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}