#include <iostream>
#include <sys/stat.h>

#include "templatedb/page.hpp"

using namespace templatedb;

LSMFile::LSMFile(unsigned _level, unsigned _k, std::string & _name){
  this->currIdx = 0;
  this->k = _k;
  this->level = _level;
  this->name = _name;
  this->file = std::fstream(_name);

  if (this->file.good()){
    char header[LSM_PAGE_HEADER_SIZE];
    this->file.read(header, LSM_PAGE_HEADER_SIZE);
    if (this->file.gcount() == LSM_PAGE_HEADER_SIZE){
      int* p = (int*)header;
      this->tupleNum = p[0];
      this->dimension = p[1];
      this->tupleSize = TUPLE_VALID_BYTE_SIZE + TUPLE_KEY_SIZE + (this->dimension * sizeof(int));
    } else{
      std::cerr << "cannot read header file: " << _name << std::endl;
    }
  }
}


inline void LSMFile::resetIndex(int idx){
  this->file.seekg(LSM_PAGE_HEADER_SIZE + idx * this->tupleSize);
}

inline int LSMFile::loadTuple(Tuple* tuple){
  this->file.read(tuple->buffer, tuple->TUPLE_SIZE);
  return this->file.gcount();
}

inline int LSMFile::loadTuple(Tuple* tuple, int idx){
  this->resetIndex(idx);
  this->loadTuple(tuple);
  return this->file.gcount();
}

std::string mergeFiles(std::vector<LSMFile*> files, const std::string & dir){
  std::vector<Tuple*> tuples(files.size());
  std::vector<bool> alive(files.size());

  // name of the temp file
  std::string hashName = "";
  for (int i = 0; i < files.size(); ++i){
    if (!files[i]->good()) return "error";
    alive[i] = true;
    tuples[i] = new Tuple(files[i]->dimension);
    files[i]->loadTuple(tuples[i]);
    hashName += files[i]->name;
  }
  std::hash<std::string> str_hash;
  std::string tempFileName = std::to_string(str_hash(hashName));
  std::fstream tempFile(dir + "/" + tempFileName);

  int count = 0;
  
  

  for (auto p : tuples){
    delete p;
  }
}