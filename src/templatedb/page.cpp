#include <iostream>
#include <sys/stat.h>
#include <bits/stdc++.h>

#include "templatedb/page.hpp"

using namespace templatedb;

LSMFile::LSMFile(unsigned _level, unsigned _k, const std::string & _name){
  this->currIdx = 0;
  this->k = _k;
  this->level = _level;
  this->name = _name;
  this->file = std::fstream(_name);

  if (!this->file.good()){
    std::ofstream openFile(_name);
    openFile.close();
    this->file.open(_name);
  }

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
  this->file.seekp(LSM_PAGE_HEADER_SIZE + idx * this->tupleSize);
}

int LSMFile::loadTuple(Tuple* tuple){
  this->file.read(tuple->buffer, tuple->TUPLE_SIZE);
  return this->file.gcount();
}

int LSMFile::loadTuple(Tuple* tuple, int idx){
  this->resetIndex(idx);
  this->loadTuple(tuple);
  return this->file.gcount();
}

void templatedb::mergeFiles(std::vector<LSMFile*> & files, const std::string & dir, std::string & newFileName, FencePointers* fp, BF::BloomFilter* bf){
  Tuple* tuples[files.size()];
  int indexes[files.size()];

  // initializations
  std::string hashName = "";
  int count = 0;
  int dimension = files[0]->dimension;
  for (int i = 0; i < files.size(); ++i){
    if (!files[i]->good()) newFileName = "error";

    indexes[i] = 0;
    files[i]->resetIndex(0);

    tuples[i] = new Tuple(files[i]->dimension);
    files[i]->loadTuple(tuples[i]);
    ++indexes[i];

    hashName += files[i]->name;
    count += files[i]->tupleNum;
  }
  std::hash<std::string> str_hash;
  std::string tempFileName = std::to_string(str_hash(hashName));
  std::ofstream tempFile(dir + "/" + tempFileName);

  // write header to the tempFile
  int header[2] = {count, dimension};
  tempFile.write((char*)header, 2*sizeof(int));
  
  int fencePointerGranularity = PAGE_SIZE/tuples[0]->TUPLE_SIZE;

  // start writing to the new file
  std::priority_queue <std::pair<int, int>> keyMinHeap; // std::priority_queue is max heap, so the keys are negated to get a min heap
  for (int i = 0; i < files.size(); ++i){
    if(tuples[i] != nullptr){
      keyMinHeap.push(std::pair<int,int>(-tuples[i]->getKey(), i));
    }
  }

  count = 0; // count the actual inserted tuples
  int prevKey; // last appended key
  int prevFileIdx = -1; // the file last appended key belongs to
  auto prevWritePos = tempFile.tellp();
  while (!keyMinHeap.empty()){
    std::pair<int, int> minKey = keyMinHeap.top();
    keyMinHeap.pop();
    int idx = minKey.second;

    if (prevFileIdx == -1 || -minKey.first != prevKey){
      prevWritePos = tempFile.tellp();
      tempFile.write(tuples[idx]->buffer, tuples[idx]->TUPLE_SIZE);

      //fence pointers and bloom filter only updated if a new key is inserted
      if (count%fencePointerGranularity == 0) {fp->add(count, tuples[idx]->getKey());} // update fencepointers
      bf->program(std::to_string(tuples[idx]->getKey())); // update bloom filter

      ++count;
      prevKey = tuples[idx]->getKey();
      prevFileIdx = idx;
    } else if (-minKey.first == prevKey){ // elimimate tuples with the same value
      if (files[idx]->level < files[prevFileIdx]->level || (files[idx]->level == files[prevFileIdx]->level && files[idx]->k > files[prevFileIdx]->k)){
        tempFile.seekp(prevWritePos);
        prevWritePos = tempFile.tellp();
        tempFile.write(tuples[idx]->buffer, tuples[idx]->TUPLE_SIZE); // rewrite previous tuple, so count does not change here
        prevKey = tuples[idx]->getKey();
        prevFileIdx = idx;
      }
    }

    if (indexes[idx] < files[idx]->tupleNum){
      files[idx]->loadTuple(tuples[idx]);
      ++indexes[idx];
      keyMinHeap.push(std::pair<int,int>(-tuples[idx]->getKey(), idx));
    }
  }

  if (count != header[0]){ // change the tupleNum in file
    tempFile.seekp(0, std::ios::beg);
    tempFile.write((char*)(&count), sizeof(int));
  }

  for (auto p : tuples){
    delete p;
  }
  tempFile.close();
  newFileName = tempFileName;
}