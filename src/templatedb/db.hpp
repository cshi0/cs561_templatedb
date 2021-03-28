#ifndef _TEMPLATEDB_DB_H_
#define _TEMPLATEDB_DB_H_

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "templatedb/operation.hpp"
#include "templatedb/lsm.hpp"

#define BUFFER_SIZE 4096

namespace templatedb
{

class DB
{
public:
    db_status status;

    DB() {};
    ~DB() {close();};

    Value get(int key);
    void put(int key, Value val);
    std::vector<Value> scan();
    std::vector<Value> scan(int min_key, int max_key);
    void del(int key);
    void del(int min_key, int max_key);

    db_status open(std::string & dir, lsm_mode mode);
    bool close();

    bool load_data_file(std::string & fname);

    std::vector<Value> execute_op(Operation op);

private:
    LSM* lsm = nullptr;
    std::string dir;
    lsm_mode mode;
    size_t value_dimensions = 0;
    
};

}   // namespace templatedb

#endif /* _TEMPLATEDB_DB_H_ */