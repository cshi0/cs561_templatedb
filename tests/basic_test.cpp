#include <iostream>
#include <vector>

#include "gtest/gtest.h"
#include "templatedb/db.hpp"


class DBTest : public ::testing::Test
{
protected:
    templatedb::DB db0;
    templatedb::DB db1;
    templatedb::DB db2;
    templatedb::DB db3;

    templatedb::Value v1 = templatedb::Value({1, 2});
    templatedb::Value v2 = templatedb::Value({6, 10});
    templatedb::Value v3 = templatedb::Value({1, 1, 5, 7});
    templatedb::Value v4 = templatedb::Value({13, 176});


    void SetUp() override 
    {
        std::string fname = "test_db"; 
        std::string name = "empty_test";
        db0.open(fname, templatedb::TIERING);
        db1.open(fname, templatedb::TIERING);
        db2.open(fname, templatedb::TIERING);
        db3.open(name, templatedb::TIERING);
        db1.put(2, v1);
        db1.put(5, v2);
        db2.put(1024, v3);
    }

    // void TearDown() override{
    //     db0.close();
    //     db1.close();
    //     db2.close();
    // }
};


// TEST_F(DBTest, IsEmptyInitially)
// {
//     EXPECT_EQ(db0.size(), 0);
// }


TEST_F(DBTest, GetFunctionality)
{
    std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_500_4.data";
    db1.load_data_file(data);
    templatedb::Value val = db1.get(2);
    EXPECT_EQ(DBTest::v1, val);
}


TEST_F(DBTest, PutAndGetFunctionality)
{
    db0.put(10, v4);
    templatedb::Value get_value = db0.get(10);

    EXPECT_EQ(DBTest::v4, get_value);
}


TEST_F(DBTest, DeleteFunctionality)
{
    db1.del(2);
    EXPECT_EQ(db1.get(2), templatedb::Value(false));
    // EXPECT_EQ(db1.size(), 1);

    db2.del(1024);
    EXPECT_EQ(db2.get(1024), templatedb::Value(false));
    // EXPECT_EQ(db2.size(), 0);
}


// TEST_F(DBTest, ScanFunctionality)
// {
//     std::vector<templatedb::Value> vals;
//     // vals = db2.scan();
//     // ASSERT_EQ(vals.size(), 1);
//     // EXPECT_EQ(vals[0], DBTest::v3);


//     std::string data = "/mnt/d/files/BU/CS 561/projects/LSM/cs561_templatedb/data/test_500_4.data";
//     db0.load_data_file(data);
//     db0.put(1, v1);
//     vals = db0.scan(1, 3);
//     ASSERT_EQ(vals.size(), 1);
//     EXPECT_EQ(vals[0], DBTest::v1);

//     db3.load_data_file(data);
//     vals = db3.scan();
//     ASSERT_EQ(vals.size(), 500);
// }


int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}