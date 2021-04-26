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
    this->dir = dir;
    this->mode = mode;
    this->init();
    return OPEN;
}


bool DB::close()
{
    if (lsm != nullptr){
        this->persist();
        delete lsm;
        this->lsm = nullptr;
    }
}

void DB::persist(){
    _writeInfo();
    _writeBuffer();
    _writeFencePointers();
    _writeBloomFilters();
}

void DB::init(){
    _readInfo();
    _readBuffer();
    _readFencePointers();
    _readBloomFilters();
}

void DB::_writeInfo(){
    std::ofstream infoFile(this->dir + "/infoFile");
    if (infoFile.good()){
        infoFile.seekp(0);
        infoFile << std::to_string(mode) << std::endl;
        infoFile << std::to_string(this->lsm->levels) << std::endl;
        auto iter = this->lsm->numFileAtLevel.cbegin() + 1;
        while (iter != this->lsm->numFileAtLevel.cend()){
            infoFile << std::to_string(*iter) << std::endl;
            ++iter;
        }
    }
    infoFile.close();
}

void DB::_readInfo(){
    std::fstream infoFile(this->dir + "/infoFile");
    if (infoFile.good()){
        infoFile.seekg(0);
        std::string mode;
        infoFile >> mode;
        this->mode = (templatedb::_lsm_mode_code) stoi(mode);
        this->lsm = new LSM(this->dir, this->mode);

        std::string levels;
        infoFile >> levels;
        this->lsm->levels = stoi(levels);

        while(infoFile.good()){
            std::string numFile;
            infoFile >> numFile;
            if (numFile == ""){ break; }
            this->lsm->numFileAtLevel.push_back(stoi(numFile));
        }
    } else{
        this->lsm = new LSM(this-> dir, this->mode);
    }
    infoFile.close();
}

void DB::_writeBuffer(){
    std::string tempName = this->lsm->writeBufferFile();
    std::rename(tempName.c_str(), (this->dir + "/bufferFile").c_str());
}

void DB::_readBuffer(){
    std::fstream bufferFile(this->dir + "/bufferFile");
    if (bufferFile.good()){
        bufferFile.seekg(0);
        int numTuple;
        bufferFile.read((char*)&numTuple, LSM_PAGE_HEADER_SIZE);
        Tuple t;
        for (int i = 0; i < numTuple; ++i){
            t.readFromFile(bufferFile);
            Value v;
            t.dumpValue(&v);
            this->lsm->inputBuffer.insert({t.getKey(), v});
        }
    }
}

void DB::_writeFencePointers(){
    std::ofstream fencePointers(this->dir + "/fencePointers");
    if (fencePointers.good()){
        fencePointers.seekp(0);
        auto FPIter = this->lsm->fencePointers.cbegin();
        while (FPIter != this->lsm->fencePointers.cend()){
            if (FPIter->first.first == 0){
                ++FPIter;
                continue;
            }

            FencePointers* fp = FPIter->second;
            if (fp != nullptr){
                //FileIdentifier
                fencePointers.write((char*)&(FPIter->first.first), sizeof(int));
                fencePointers.write((char*)&(FPIter->first.second), sizeof(int));
                fp->serialize(fencePointers);
            }

            ++FPIter;
        }
    }
    fencePointers.close();
}

void DB::_readFencePointers(){
    std::fstream fencePointers(this->dir + "/fencePointers");
    if (fencePointers.good()){
        fencePointers.seekg(0);
        while (fencePointers.good()){
            int level, k;
            size_t objectSize;
            fencePointers.read((char*)&level, sizeof(int));
            fencePointers.read((char*)&k, sizeof(int));
            
            FencePointers* newFP = new FencePointers();
            if (!newFP->deserialize(fencePointers)) {free(newFP);continue;}

            this->lsm->fencePointers[{level, k}] = (FencePointers*)newFP;
        }
    }
    fencePointers.close();
}

void DB::_writeBloomFilters(){
    std::ofstream bloomFilters(this->dir + "/bloomFilters", std::ios::binary);
    if (bloomFilters.good()){
        bloomFilters.seekp(0);
        auto BFIter = this->lsm->bloomfilters.cbegin();
        while (BFIter != this->lsm->bloomfilters.cend()){
            if (BFIter->first.first == 0){
                ++BFIter;
                continue;
            }
            BF::BloomFilter* bf = BFIter->second;
            if (bf != nullptr){
                //FileIdentifier
                bloomFilters.write((char*)&(BFIter->first.first), sizeof(int));
                bloomFilters.write((char*)&(BFIter->first.second), sizeof(int));
                bf->serialize(bloomFilters);
            }
            
            ++BFIter;
        }
    }
    bloomFilters.close();
}

void DB::_readBloomFilters(){
    std::fstream bloomFilters(this->dir + "/bloomFilters");
    if(bloomFilters.good()){
        bloomFilters.seekg(0);
        while (bloomFilters.good()){
            int level, k;
            size_t objectSize;
            bloomFilters.read((char*)&level, sizeof(int));
            bloomFilters.read((char*)&k, sizeof(int));
            
            BF::BloomFilter* newBF = new BF::BloomFilter();
            if (!newBF->deserialize(bloomFilters)){delete newBF;continue;}

            this->lsm->bloomfilters[{level,k}] = (BF::BloomFilter*)newBF;
        }
    }
    bloomFilters.close();
}