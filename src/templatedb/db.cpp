#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include "templatedb/db.hpp"

using namespace templatedb;


Value DB::get(int key)
{
    if (this->lsm == nullptr){
        return Value(false);
    }
    return lsm->get(key);
}


void DB::put(int key, Value val)
{
    if (this->lsm == nullptr){
        return;
    }
    lsm->put(key, val);
}


std::vector<Value> DB::scan()
{
    if (this->lsm == nullptr){
        return std::vector<Value>();
    }
    return lsm->scan();
}


std::vector<Value> DB::scan(int min_key, int max_key)
{
    if (this->lsm == nullptr){
        return std::vector<Value>();
    }
    return lsm->scan(min_key, max_key);
}


void DB::del(int key)
{
    if (this->lsm == nullptr){
        return;
    }
    lsm->put(key, Value(false));
}


void DB::del(int min_key, int max_key)
{
    for (int i = min_key; i <= max_key; ++i){
        lsm->put(i, Value(false));
    }
}

std::vector<Value> DB::execute_op(Operation op)
{
    std::vector<Value> results;
    if (op.type == GET)
    {
        results.push_back(this->get(op.key));
    }
    else if (op.type == PUT)
    {
        this->put(op.key, Value(op.args));
    }
    else if (op.type == SCAN)
    {
        results = this->scan(op.key, op.args[0]);
    }
    else if (op.type == DELETE)
    {
        if ( op.args.size()>0 ){
            this->del(op.key, op.args[0]);
        }
        else
            this->del(op.key);
    }

    return results;
}


bool DB::load_data_file(std::string & fname)
{
    std::fstream fid(fname);

    int key;
    int line_num = 0;
    std::string line;
    if (fid.is_open())
    {
        std::getline(fid, line); // First line is rows, col
    } else{
        fprintf(stderr, "Unable to read %s\n", fname.c_str());
        return false;
    }

    if (lsm == nullptr){
        std::stringstream linestream(line);
        std::string item;

        std::getline(linestream, item, ' '); // row
        std::getline(linestream, item, ' '); // dimension

        this->lsm = new LSM(this->dir, this->mode);
    }

    while (std::getline(fid, line)){
        line_num++;
        std::stringstream linestream(line);
        std::string item;

        std::getline(linestream, item, ' ');
        std::string op_code = item;

        std::getline(linestream, item, ' ');
        key = stoi(item);
        std::vector<int> items;
        while(std::getline(linestream, item, ' '))
        {
            items.push_back(stoi(item));
        }
        this->put(key, Value(items));
    }

    return true;
}


db_status DB::open(std::string & dir, lsm_mode mode)
{
    if (this->lsm != nullptr){
        return OPEN;
    }

    struct stat info;

    if( !(stat(dir.c_str(), &info) == 0 && S_ISDIR(info.st_mode)) ){
        if (mkdir(dir.c_str(), 0777) != 0){
            printf( "cannot create %s\n", dir.c_str() );
            printf("%d", errno);
            return ERROR_OPEN;
        }
    }
    this->lsm = new LSM(dir, mode);
    this->dir = dir;
    this->mode = mode;
    return OPEN;
}


bool DB::close()
{
    if (lsm != nullptr){
        delete lsm;
    }
}