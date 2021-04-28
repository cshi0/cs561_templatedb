#include <string>
#include <iostream>
#include <vector>

#include "gtest/gtest.h"
#include "templatedb/db.hpp"

TEST(StorageTest, Tiering)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db2"; 
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_100000000_4.data";
    db.load_data_file(data);
    db.close();
}

TEST(StorageTest, Leveling)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db2"; 
    templatedb::DB db;
    db.open(fname, templatedb::LEVELING);
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_100000000_4.data";
    db.load_data_file(data);
    db.close();
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}