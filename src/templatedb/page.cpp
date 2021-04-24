#include <iostream>
#include <queue>
#include <sys/stat.h>

#include "templatedb/page.hpp"

using namespace templatedb;

LSMFile::LSMFile(unsigned _level, unsigned _k, const std::string & _name){
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
    } else{
      std::cerr << "cannot read header file: " << _name << std::endl;
    }
  }
}


inline void LSMFile::resetIndex(streampos idx){
  this->file.seekg(idx);
  this->file.seekp(idx);
}

int LSMFile::loadTuple(Tuple* tuple){
  return tuple->readFromFile(this->file);
}

int LSMFile::loadTuple(Tuple* tuple, streampos idx){
  this->resetIndex(idx);
  return tuple->readFromFile(this->file);
}

void templatedb::mergeFiles(std::vector<LSMFile*> & files, const std::string & dir, std::string & newFileName, FencePointers* fp, BF::BloomFilter* bf){
  Tuple* tuples[files.size()];
  int indexes[files.size()];

  // initializations
  std::string hashName = "";
  int count = 0;
  for (int i = 0; i < files.size(); ++i){
    if (!files[i]->good()) newFileName = "error";

    indexes[i] = 0;
    files[i]->resetIndex(0);

    char header[LSM_PAGE_HEADER_SIZE];
    files[i]->file.read(header, LSM_PAGE_HEADER_SIZE);

    tuples[i] = new Tuple();
    files[i]->loadTuple(tuples[i]);
    ++indexes[i];

    hashName += files[i]->name;
    count += files[i]->tupleNum;
  }
  std::hash<std::string> str_hash;
  std::string tempFileName = std::to_string(str_hash(hashName));
  std::ofstream tempFile(dir + "/" + tempFileName);

  // write header to the tempFile
  int header[1] = {count};
  tempFile.write((char*)header, sizeof(int));

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
      tuples[idx]->writeToFile(&tempFile);

      //fence pointers and bloom filter only updated if a new key is inserted
      if (count%FENCEPOINTER_GRANULARITY == 0) {fp->add(prevWritePos, tuples[idx]->getKey());} // update fencepointers
      bf->program(std::to_string(tuples[idx]->getKey())); // update bloom filter

      ++count;
      prevKey = tuples[idx]->getKey();
      prevFileIdx = idx;
    } else if (-minKey.first == prevKey){ // elimimate tuples with the same value
      if (files[idx]->level < files[prevFileIdx]->level || (files[idx]->level == files[prevFileIdx]->level && files[idx]->k > files[prevFileIdx]->k)){ // if newer
        tempFile.seekp(prevWritePos);
        prevWritePos = tempFile.tellp();
        tuples[idx]->writeToFile(&tempFile); // rewrite previous tuple, so count does not change here
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