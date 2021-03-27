#include <unordered_set>

#include "templatedb/lsm.hpp"

using namespace templatedb;

LSMFile* LSM::_getFile(int level, int k){
  std::string fileName = this->_getFileName(level, k);
  auto mapIter = this->files.find(FileIdentifer(level, k));
  if (mapIter != this->files.cend()){
    return mapIter->second;
  } else{
    LSMFile* file = new LSMFile(level, k, fileName);
    this->files.insert({FileIdentifer(level, k), file});
    return file;
  }
}

Value LSM::_TieringGet(int key){
  // search input buffer
  auto bufferElem = this->inputBuffer.find(key);
  if (bufferElem != this->inputBuffer.cend()){
    return bufferElem->second;
  }

  Value result;
  for (int i = 1; i <= this->levels; ++i){
    for (int k = this->numFileAtLevel[i]; i > 0; --i){
      FileIdentifer fileChecking = FileIdentifer(i,k);

      auto bloomFilterElem = this->bloomfilters.find(fileChecking);
      if (bloomFilterElem != this->bloomfilters.cend() && bloomFilterElem->second->query(std::to_string(key))){
        FencePointers* fencePointer = (this->fencePointers.find(fileChecking))->second;
        LSMFile* file = this->_getFile(i, k);

        std::pair<int,int> fence = fencePointer->getOffset(key); // get the fence region
        int trav = fence.first;
        int last = fence.second < INT32_MAX ? fence.second : file->tupleNum - 1; // check whether the right end of the fence is at the end of the file;

        // start traverse the fence
        file->resetIndex(trav);
        Tuple t(file->dimension);
        do {
          file->loadTuple(&t);
          ++trav;
        } while (t.getKey() != key && trav <= last);

        // tuple found
        if (t.getKey() == key){
          t.dumpValue(&result);
          return result;
        }
      }
    }
  }

  return Value(false); // did't find out
}

void LSM::_TieringPut(int key, Value val){
  this->inputBuffer.insert({key, val});
  if (this->inputBuffer.size() >= this->BUFFER_PAIR_LIMIT){
    this->_TieringPushLevel(0);
  }
}

std::vector<Value> LSM::_TieringScan(){
  std::unordered_set<int> keyFound;
  std::vector<Value> result;
  Tuple t(this->DIMENSION);

  // scan the input buffer first
  auto inputBufferIter = this->inputBuffer.cbegin();
  while(inputBufferIter != this->inputBuffer.cend()){
    if (keyFound.find(inputBufferIter->first) != keyFound.cend()){ // if it is a new key
      result.push_back(inputBufferIter->second);
    }
    ++inputBufferIter;
  }

  // scan the files
  for (int i = 1; i <= this->levels; ++i){
    for (int k = this->numFileAtLevel[i]; i > 0; --i){
      FileIdentifer fileChecking = FileIdentifer(i,k);
      LSMFile* file = this->_getFile(i, k);
      file->resetIndex(0);

      for (int i = 0; i < file->tupleNum; ++i){
        file->loadTuple(&t);
        if (keyFound.find(t.getKey()) != keyFound.cend()){ // if it is a new key
          Value newValue;
          t.dumpValue(&newValue);
          result.push_back(newValue);
        }
      }
    }
  }

  return result;
}

std::vector<Value> LSM::_TieringScan(int min_key, int max_key){
  std::unordered_set<int> keyFound;
  std::vector<Value> result;
  Tuple t(this->DIMENSION);

  // scan the input buffer first
  auto inputBufferIter = this->inputBuffer.cbegin();
  while(inputBufferIter != this->inputBuffer.cend()){
    if (keyFound.find(inputBufferIter->first) != keyFound.cend() && inputBufferIter->first >= min_key && inputBufferIter->first <= max_key){ // if it is a new key
      result.push_back(inputBufferIter->second);
    }
    ++inputBufferIter;
  }

  // scan the files
  for (int i = 1; i <= this->levels; ++i){
    for (int k = this->numFileAtLevel[i]; i > 0; --i){
      FileIdentifer fileChecking = FileIdentifer(i,k);
      FencePointers* fp = (this->fencePointers.find(fileChecking))->second;
      std::pair<int,int> minPair = fp->getOffset(min_key);
      std::pair<int,int> maxPair = fp->getOffset(max_key);
      LSMFile* file = this->_getFile(i, k);

      int trav = minPair.first;
      int end = maxPair.second < INT32_MAX ? maxPair.second : file->tupleNum - 1; // check whether the right end of the fence is at the end of the file;
      
      file->resetIndex(trav);
      for (trav; i <= end; ++i){
        file->loadTuple(&t);
        if (keyFound.find(t.getKey()) != keyFound.cend() && t.getKey() >= min_key && t.getKey() <= max_key){ // if it is a new key
          Value newValue;
          t.dumpValue(&newValue);
          result.push_back(newValue);
        }
      }
    }
  }

  return result;
}

void LSM::_TieringPushLevel(int level){
  if (level == 0){
    std::ofstream tempFile;
    if (this->levels > 0 && this->numFileAtLevel[1] == this->_LevelFileLimit(1)){
      this->_TieringPushLevel(1);
    }
    tempFile.open(this->_getFileName(1, ++this->numFileAtLevel[1]));

    Tuple tuple(this->DIMENSION);
    int valueBuffer[(1 + this->DIMENSION) * sizeof(int)];
    auto inputBufferIter = this->inputBuffer.cbegin();

    tempFile.seekp(0);
    while(inputBufferIter != this->inputBuffer.cend()){
      valueBuffer[0] = inputBufferIter->first;
      int i = 1;
      for (int num : (inputBufferIter->second.items)){
        valueBuffer[i] = num;
      }
      tuple.loadFromArray(inputBufferIter->second.visible, valueBuffer, (1 + this->DIMENSION) * sizeof(int));
      tuple.writeToFile(&tempFile);
    }
  } else{
    if (this->levels > level && this->numFileAtLevel[level + 1] == this->_LevelFileLimit(level + 1)){
      this->_TieringPushLevel(level + 1);
    }
    
    std::vector<LSMFile*> files;
    for (int k = 1; k <= this->numFileAtLevel[level]; ++k){
      files.push_back(this->_getFile(level, k));

      auto BFIter = this->bloomfilters.find({level, k});
      if (BFIter != this->bloomfilters.cend()){
        delete BFIter->second;
      }
      this->bloomfilters.erase({level, k});

      auto FPIter = this->fencePointers.find({level, k});
      if (FPIter != this->fencePointers.cend()){
        delete FPIter->second;
      }
      this->fencePointers.erase({level, k});
    }
    std::string newFileName;
    BF::BloomFilter* newBF = new BF::BloomFilter();
    FencePointers* newFP = new FencePointers();
    mergeFiles(files, this->DIR, newFileName, newFP, newBF);

    this->numFileAtLevel[level] = 0;
    rename(newFileName.c_str(), (this->_getFileName(level + 1, ++this->numFileAtLevel[level + 1])).c_str());
    this->bloomfilters.insert({{level + 1, this->numFileAtLevel[level + 1]}, newBF});
    this->fencePointers.insert({{level + 1, this->numFileAtLevel[level + 1]}, newFP});


    for (auto file : files){
      this->files.erase({file->level, file->k});
      std::remove(file->name.c_str());
      delete file;
    }
  }
}

// leveling
Value LSM::_LevelingGet(int key){
  // search input buffer
  auto bufferElem = this->inputBuffer.find(key);
  if (bufferElem != this->inputBuffer.cend()){
    return bufferElem->second;
  }

  Value result;
  for (int i = 1; i <= levels; ++i){
    FileIdentifer fileChecking = FileIdentifer(i,0);

    auto bloomFilterElem = this->bloomfilters.find(fileChecking);
    if (bloomFilterElem != this->bloomfilters.cend() && bloomFilterElem->second->query(std::to_string(key))){
      FencePointers* fencePointer = (this->fencePointers.find(fileChecking))->second;
      LSMFile* file = this->_getFile(i, 0);

      std::pair<int,int> fence = fencePointer->getOffset(key); // get the fence region
      int trav = fence.first;
      int last = fence.second < INT32_MAX ? fence.second : file->tupleNum - 1; // check whether the right end of the fence is at the end of the file;

      // start traverse the fence
      file->resetIndex(trav);
      Tuple t(file->dimension);
      do {
        file->loadTuple(&t);
        ++trav;
      } while (t.getKey() != key && trav <= last);

      // tuple found
      if (t.getKey() == key){
        t.dumpValue(&result);
        return result;
      }
    }
  }

  return Value(false); // did't find out
}

void LSM::_LevelingPut(int key, Value val){
  this->inputBuffer.insert({key, val});
  if (this->inputBuffer.size() >= this->BUFFER_PAIR_LIMIT){
    this->_LevelingPushLevel(0);
  }
}

std::vector<Value> LSM::_LevelingScan(){
  std::unordered_set<int> keyFound;
  std::vector<Value> result;
  Tuple t(this->DIMENSION);

  // scan the input buffer first
  auto inputBufferIter = this->inputBuffer.cbegin();
  while(inputBufferIter != this->inputBuffer.cend()){
    if (keyFound.find(inputBufferIter->first) != keyFound.cend()){ // if it is a new key
      result.push_back(inputBufferIter->second);
    }
    ++inputBufferIter;
  }

  // scan the files
  for (int i = 1; i <= this->levels; ++i){
    FileIdentifer fileChecking = FileIdentifer(i,0);
    LSMFile* file = this->_getFile(i, 0);
    file->resetIndex(0);

    for (int i = 0; i < file->tupleNum; ++i){
      file->loadTuple(&t);
      if (keyFound.find(t.getKey()) != keyFound.cend()){ // if it is a new key
        Value newValue;
        t.dumpValue(&newValue);
        result.push_back(newValue);
      }
    }
  }

  return result;
}

std::vector<Value> LSM::_LevelingScan(int min_key, int max_key){
  std::unordered_set<int> keyFound;
  std::vector<Value> result;
  Tuple t(this->DIMENSION);

  // scan the input buffer first
  auto inputBufferIter = this->inputBuffer.cbegin();
  while(inputBufferIter != this->inputBuffer.cend()){
    if (keyFound.find(inputBufferIter->first) != keyFound.cend() && inputBufferIter->first >= min_key && inputBufferIter->first <= max_key){ // if it is a new key
      result.push_back(inputBufferIter->second);
    }
    ++inputBufferIter;
  }

  // scan the files
  for (int i = 1; i <= this->levels; ++i){
    FileIdentifer fileChecking = FileIdentifer(i,0);
    FencePointers* fp = (this->fencePointers.find(fileChecking))->second;
    std::pair<int,int> minPair = fp->getOffset(min_key);
    std::pair<int,int> maxPair = fp->getOffset(max_key);
    LSMFile* file = this->_getFile(i, 0);

    int trav = minPair.first;
    int end = maxPair.second < INT32_MAX ? maxPair.second : file->tupleNum - 1; // check whether the right end of the fence is at the end of the file;
    
    file->resetIndex(trav);
    for (trav; i <= end; ++i){
      file->loadTuple(&t);
      if (keyFound.find(t.getKey()) != keyFound.cend() && t.getKey() >= min_key && t.getKey() <= max_key){ // if it is a new key
        Value newValue;
        t.dumpValue(&newValue);
        result.push_back(newValue);
      }
    }
  }

  return result;
}

void LSM::_LevelingPushLevel(int level){
  if (level == 0){
    std::ofstream tempFile;
    if (this->levels > 0 && this->numFileAtLevel[1] == this->_LevelFileLimit(1)){
      this->_LevelingPushLevel(1);
    }
    std::string tempFileName = "tempFile";
    tempFile.open(tempFileName);

    Tuple tuple(this->DIMENSION);
    int valueBuffer[(1 + this->DIMENSION) * sizeof(int)];
    auto inputBufferIter = this->inputBuffer.cbegin();

    tempFile.seekp(0);
    while(inputBufferIter != this->inputBuffer.cend()){
      valueBuffer[0] = inputBufferIter->first;
      int i = 1;
      for (int num : (inputBufferIter->second.items)){
        valueBuffer[i] = num;
      }
      tuple.loadFromArray(inputBufferIter->second.visible, valueBuffer, (1 + this->DIMENSION) * sizeof(int));
      tuple.writeToFile(&tempFile);

      FileIdentifer nextLevelFile = {level + 1, 1};
      std::vector<LSMFile*> files;
      files.push_back(this->_getFile(level + 1, 1));
      files.push_back(&LSMFile(0,0, tempFileName));
      
      std::string newFileName;
      BF::BloomFilter* newBF = new BF::BloomFilter();
      FencePointers* newFP = new FencePointers();
      mergeFiles(files, this->DIR, newFileName, newFP, newBF);
      this->numFileAtLevel[level + 1] += 1;

      auto BFIter = this->bloomfilters.find(nextLevelFile);
      if (BFIter != this->bloomfilters.cend()){
        delete BFIter->second;
        this->bloomfilters.erase(nextLevelFile);
      }
      this->bloomfilters.insert({nextLevelFile, newBF});

      auto FPIter = this->fencePointers.find(nextLevelFile);
      if (FPIter != this->fencePointers.cend()){
        delete FPIter->second;
        this->fencePointers.erase(nextLevelFile);
      }
      this->fencePointers.insert({nextLevelFile, newFP});

      for (auto file : files){
        this->files.erase({file->level, file->k});
        std::remove(file->name.c_str());
        delete file;
      }
      std::rename(newFileName.c_str(), this->_getFileName(level + 1, 1).c_str());
    }
  } else{
    if (this->levels > level && this->numFileAtLevel[level + 1] == this->_LevelFileLimit(level + 1)){
      this->_LevelingPushLevel(level + 1);
    }
    
    FileIdentifer thisLevelFile = {level, 1};
    FileIdentifer nextLevelFile = {level + 1, 1};
    std::vector<LSMFile*> files;
    files.push_back(this->_getFile(level + 1, 1));
    files.push_back(this->_getFile(level, 1));
    
    std::string newFileName;
    BF::BloomFilter* newBF = new BF::BloomFilter();
    FencePointers* newFP = new FencePointers();
    mergeFiles(files, this->DIR, newFileName, newFP, newBF);
    this->numFileAtLevel[level] = 0;
    this->numFileAtLevel[level + 1] += 1;

    auto BFIter = this->bloomfilters.find(nextLevelFile);
    if (BFIter != this->bloomfilters.cend()){
      delete BFIter->second;
      this->bloomfilters.erase(nextLevelFile);
    }
    this->bloomfilters.insert({nextLevelFile, newBF});
    BFIter = this->bloomfilters.find(thisLevelFile);
    if (BFIter != this->bloomfilters.cend()){
      delete BFIter->second;
      this->bloomfilters.erase(thisLevelFile);
    }

    auto FPIter = this->fencePointers.find(nextLevelFile);
    if (FPIter != this->fencePointers.cend()){
      delete FPIter->second;
      this->fencePointers.erase(nextLevelFile);
    }
    this->fencePointers.insert({nextLevelFile, newFP});
    FPIter = this->fencePointers.find(thisLevelFile);
    if (FPIter != this->fencePointers.cend()){
      delete FPIter->second;
      this->fencePointers.erase(thisLevelFile);
    }

    for (auto file : files){
      this->files.erase({file->level, file->k});
      std::remove(file->name.c_str());
      delete file;
    }
    std::rename(newFileName.c_str(), this->_getFileName(level + 1, 1).c_str());
  }
}