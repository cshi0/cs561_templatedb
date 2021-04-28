#include <string>
#include <iostream>
#include <vector>

#include "gtest/gtest.h"
#include "templatedb/db.hpp"

TEST(StorageTest, Tiering1000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db2";
    std::string workload = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_1000_4_65535.wl";
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file(workload);
    int count = 0;
    for (auto op : ops){
      // printf("%d\n", ++count);
      db.execute_op(op);
    }
    db.close();
}
TEST(StorageTest, Tiering5000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db2";
    std::string workload = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_5000_4_65535.wl";
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file(workload);
    for (auto op : ops){
      db.execute_op(op);
    }
    db.close();
}
TEST(StorageTest, Tiering10000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db2";
    std::string workload = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_10000_4_65535.wl";
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file(workload);
    for (auto op : ops){
      db.execute_op(op);
    }
    db.close();
}
TEST(StorageTest, Tiering15000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db2";
    std::string workload = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_15000_4_65535.wl";
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file(workload);
    for (auto op : ops){
      db.execute_op(op);
    }
    db.close();
}
TEST(StorageTest, Tiering20000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_tiering_db2";
    std::string workload = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_20000_4_65535.wl";
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file(workload);
    for (auto op : ops){
      db.execute_op(op);
    }
    db.close();
}

TEST(StorageTest, Leveling1000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db2"; 
    std::string workload = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_1000_4_65535.wl";
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file(workload);
    for (auto op : ops){
      db.execute_op(op);
    }
    db.close();
}
TEST(StorageTest, Leveling5000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db2"; 
    std::string workload = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_5000_4_65535.wl";
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file(workload);
    for (auto op : ops){
      db.execute_op(op);
    }
    db.close();
}
TEST(StorageTest, Leveling10000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db2"; 
    std::string workload = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_10000_4_65535.wl";
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file(workload);
    for (auto op : ops){
      db.execute_op(op);
    }
    db.close();
}
TEST(StorageTest, Leveling15000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db2"; 
    std::string workload = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_15000_4_65535.wl";
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file(workload);
    for (auto op : ops){
      db.execute_op(op);
    }
    db.close();
}
TEST(StorageTest, Leveling20000)
{
    std::string fname = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/test_leveling_db2"; 
    std::string workload = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_20000_4_65535.wl";
    templatedb::DB db;
    db.open(fname, templatedb::TIERING);
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file(workload);
    for (auto op : ops){
      db.execute_op(op);
    }
    db.close();
}


int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}