#include <unordered_set>

#include "templatedb/lsm.hpp"

using namespace templatedb;

LSMFile* LSM::_getFile(int level, int k){
  std::string fileName = this->_getFileName(level, k);
  auto lsmFile = this->files[FileIdentifier(level, k)];
  if (lsmFile == nullptr){
    lsmFile = new LSMFile(level, k, fileName);
    this->files[FileIdentifier(level, k)] = lsmFile;
  }
  return lsmFile;
}

void LSM::_levelingMerge(int level, std::vector<LSMFile*> files){
  if (this->levels <= level){
    std::rename(this->_getFileName(level, 1).c_str(), this->_getFileName(level + 1, 1).c_str());
    this->levels += 1;
    this->numFileAtLevel[level] = 0;
    this->numFileAtLevel.push_back(1);
    this->bloomfilters[{level+1,1}] = this->bloomfilters[{level, 1}];
    this->bloomfilters[{level, 1}] = nullptr;
    this->fencePointers[{level+1,1}] = this->fencePointers[{level, 1}];
    this->fencePointers[{level, 1}] = nullptr;
    this->files[{level, 1}] = nullptr;
    return;
  } else if (this->numFileAtLevel[level+1] == 0){
    std::rename(this->_getFileName(level, 1).c_str(), this->_getFileName(level + 1, 1).c_str());
    this->numFileAtLevel[level] = 0;
    this->numFileAtLevel[level+1] = 1;
    this->bloomfilters[{level+1,1}] = this->bloomfilters[{level, 1}];
    this->bloomfilters[{level, 1}] = nullptr;
    this->fencePointers[{level+1,1}] = this->fencePointers[{level, 1}];
    this->fencePointers[{level, 1}] = nullptr;
    this->files[{level, 1}] = nullptr;
    return;
  }
  
  FileIdentifier thisLevelFile = {level, 1};
  FileIdentifier nextLevelFile = {level + 1, 1};

  files.push_back(this->_getFile(level + 1, 1));
  if (level > 0){
    files.push_back(this->_getFile(level, 1));
  }
  
  std::string newFileName;
  BF::BloomFilter* newBF = new BF::BloomFilter();
  FencePointers* newFP = new FencePointers();
  mergeFiles(files, this->DIR, newFileName, newFP, newBF);
  this->numFileAtLevel[level] = 0;
  this->numFileAtLevel[level + 1] += 1;

  auto nextLevelBF = this->bloomfilters[nextLevelFile];
  if (nextLevelBF != nullptr){
    delete nextLevelBF;
    this->bloomfilters.erase(nextLevelFile);
  }
  this->bloomfilters[nextLevelFile] = newBF;
  auto thisLevelBF = this->bloomfilters[thisLevelFile];
  if (thisLevelBF != nullptr){
    delete thisLevelBF;
    this->bloomfilters[thisLevelFile] = nullptr;
  }

  auto nextLevelFP = this->fencePointers[nextLevelFile];
  if (nextLevelFP != nullptr){
    delete nextLevelFP;
  }
  this->fencePointers[nextLevelFile] = newFP;
  auto thisLevelFP = this->fencePointers[thisLevelFile];
  if (thisLevelFP != nullptr){
    delete thisLevelFP;
    this->fencePointers[thisLevelFile] = nullptr;
  }

  for (auto file : files){
    this->files[{file->level, file->k}] = nullptr;
    std::remove(file->name.c_str());
    delete file;
  }
  std::rename((this->DIR + "/" + newFileName).c_str(), this->_getFileName(level + 1, 1).c_str());
  LSMFile* newFile = new LSMFile(level+1, 1, this->_getFileName(level+1,1));
  this->files[{newFile->level, newFile->k}] = newFile;
}

std::string LSM::writeBufferFile(){
  std::ofstream tempFile;
  std::string tempFileName = this->DIR + "/tempFile";
  tempFile.open(tempFileName);
  
  Tuple tuple;
  int size = 0;
  int count = 0;
  int valueBuffer[MAX_TUPLE_SIZE];
  auto inputBufferIter = this->inputBuffer.cbegin();

  FileIdentifier newFileIdentifier;
  if (this->MODE == templatedb::LEVELING){
    newFileIdentifier = {1,1};
  } else{
    newFileIdentifier = {1, this->numFileAtLevel[1] + 1};
  }
  BF::BloomFilter* newBF = new BF::BloomFilter();
  FencePointers* newFP = new FencePointers();
  this->bloomfilters[newFileIdentifier] = newBF;
  this->fencePointers[newFileIdentifier] = newFP;

  tempFile.seekp(LSM_PAGE_HEADER_SIZE);
  while(inputBufferIter != this->inputBuffer.cend()){
    valueBuffer[0] = inputBufferIter->first;
    size = 1;
    for (int num : (inputBufferIter->second.items)){
      valueBuffer[size++] = num;
    }
    tuple.loadFromArray(inputBufferIter->second.visible, valueBuffer, size);
    
    std::string key = std::to_string(inputBufferIter->first);
    newBF->program(key);
    if (count % FENCEPOINTER_GRANULARITY == 0) {newFP->add(tempFile.tellp(), inputBufferIter->first);}

    tuple.writeToFile(&tempFile);
    ++count;
    ++inputBufferIter;
  }

  tempFile.seekp(0, std::ios::beg);
  tempFile.write((char*)&count, 4);

  inputBuffer.byte_size = 0;
  inputBuffer.clear();

  return tempFileName;
}

Value LSM::_TieringGet(int key){
  // search input buffer
  Value bufferValue = this->inputBuffer.find(key);
  if (bufferValue.valid == true){
    return bufferValue;
  }

  Value result;
  for (int i = 1; i <= this->levels; ++i){
    for (int k = this->numFileAtLevel[i]; k > 0; --k){
      FileIdentifier fileChecking = FileIdentifier(i,k);

      auto bloomFilterElem = this->bloomfilters[fileChecking];
      if (bloomFilterElem != nullptr && bloomFilterElem->query(std::to_string(key))){
        FencePointers* fencePointer = this->fencePointers[fileChecking];
        LSMFile* file = this->_getFile(i, k);

        std::pair<streampos, streampos> fence = fencePointer->getOffset(key); // get the fence region
        streampos trav = fence.first;

        // find file end
        file->file.seekg(0, std::ios::end);
        streampos end = file->file.tellg();
        streampos last = (std::streamoff) fence.second != -1 ? fence.second : end; // check whether the right end of the fence is at the end of the file;

        // start traverse the fence
        file->resetIndex(trav);
        Tuple t;
        do {
          file->loadTuple(&t);
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
  if (this->inputBuffer.size() + val.getTupleSize() > PAGE_SIZE){
    this->_TieringPushLevel(0);
  }
  this->inputBuffer.insert({key,val});
}

std::vector<Value> LSM::_TieringScan(){
  std::unordered_set<int> keyFound;
  std::vector<Value> result;
  Tuple t;

  // scan the input buffer first
  auto inputBufferIter = this->inputBuffer.cbegin();
  while(inputBufferIter != this->inputBuffer.cend()){
    if (keyFound.find(inputBufferIter->first) == keyFound.cend()){ // if it is a new key
      result.push_back(inputBufferIter->second);
      keyFound.insert(inputBufferIter->first);
    }
    ++inputBufferIter;
  }

  // scan the files
  for (int i = 1; i <= this->levels; ++i){
    for (int k = this->numFileAtLevel[i]; k > 0; --k){
      FileIdentifier fileChecking = FileIdentifier(i,k);
      LSMFile* file = this->_getFile(i, k);

      file->file.seekg(0, std::ios::end);
      streampos end = file->file.tellg();

      file->resetIndex(0);
      int numTuples;
      file->file.read((char*)&numTuples, LSM_PAGE_HEADER_SIZE);
      streampos trav = file->file.tellg();

      int count = 0;
      while (trav < end && count < numTuples){
        int gcount = file->loadTuple(&t);
        ++count;
        trav = file->file.tellg();
        if (keyFound.find(t.getKey()) == keyFound.cend()){ // if it is a new key
          Value newValue;
          t.dumpValue(&newValue);
          if (newValue.valid) {
            result.push_back(newValue);
            keyFound.insert(inputBufferIter->first);
          }
        }
      }
    }
  }

  return result;
}

std::vector<Value> LSM::_TieringScan(int min_key, int max_key){
  std::unordered_set<int> keyFound;
  std::vector<Value> result;
  Tuple t;

  // scan the input buffer first
  auto inputBufferIter = this->inputBuffer.cbegin();
  while(inputBufferIter != this->inputBuffer.cend()){
    if (keyFound.find(inputBufferIter->first) == keyFound.cend() && inputBufferIter->first >= min_key && inputBufferIter->first <= max_key){ // if it is a new key
      result.push_back(inputBufferIter->second);
      keyFound.insert(inputBufferIter->first);
    }
    ++inputBufferIter;
  }

  // scan the files
  for (int i = 1; i <= this->levels; ++i){
    for (int k = this->numFileAtLevel[i]; k > 0; --k){
      FileIdentifier fileChecking = FileIdentifier(i,k);
      FencePointers* fp = (this->fencePointers.find(fileChecking))->second;
      std::pair<streampos,streampos> minPair = fp->getOffset(min_key);
      std::pair<streampos,streampos> maxPair = fp->getOffset(max_key);
      LSMFile* file = this->_getFile(i, k);

      streampos trav = minPair.first;
      file->file.seekg(0, std::ios::end);
      streampos end = (std::streamoff) maxPair.second != -1 ? maxPair.second : file->file.end; // check whether the right end of the fence is at the end of the file;

      if ((std::streamoff) trav == -1) {continue;}// min less than the minimum in the file
      
      int prevKey = INT32_MIN;
      file->resetIndex(trav);
      while (trav <= end){
        file->loadTuple(&t);
        trav = file->file.tellg();
        if (keyFound.find(t.getKey()) == keyFound.cend() && t.getKey() >= min_key && t.getKey() <= max_key && t.getKey() > prevKey){ // if it is a new key
          Value newValue;
          t.dumpValue(&newValue);
          if (newValue.valid) {
            result.push_back(newValue);
            keyFound.insert(inputBufferIter->first);
          }
        }
        prevKey = t.getKey();
      }
    }
  }

  return result;
}

void LSM::_TieringPushLevel(int level){
  if (this->levels > level && this->numFileAtLevel[level + 1] == this->_LevelFileLimit(level + 1)){
      this->_TieringPushLevel(level + 1);
  }
  if (level == 0){
    std::string tempFileName = this->writeBufferFile();

    if (this->numFileAtLevel.size() <= level+1){
      std::rename(tempFileName.c_str(), this->_getFileName(level + 1, 1).c_str());
      this->levels += 1;
      this->numFileAtLevel.push_back(1);
    } else if (this->numFileAtLevel[level+1] != 0){
      std::rename(tempFileName.c_str(), this->_getFileName(level + 1, ++this->numFileAtLevel[level+1]).c_str());
    } else{
      std::rename(tempFileName.c_str(), this->_getFileName(level + 1, ++this->numFileAtLevel[level+1]).c_str());
      this->numFileAtLevel[1] = 1;
    }
  } else{
    std::vector<LSMFile*> files;
    for (int k = 1; k <= this->numFileAtLevel[level]; ++k){
      files.push_back(this->_getFile(level, k));

      auto BFIter = this->bloomfilters[{level, k}];
      if (BFIter != nullptr){
        delete BFIter;
        this->bloomfilters[{level, k}] = nullptr;
      }

      auto FPIter = this->fencePointers[{level, k}];
      if (FPIter != nullptr){
        delete FPIter;
        this->fencePointers[{level, k}] = nullptr;
      }
    }

    std::string newFileName;
    BF::BloomFilter* newBF = new BF::BloomFilter();
    FencePointers* newFP = new FencePointers();
    mergeFiles(files, this->DIR, newFileName, newFP, newBF);

    this->numFileAtLevel[level] = 0;
    if (this->numFileAtLevel.size() <= level+1){
      std::rename((this->DIR + "/" + newFileName).c_str(), this->_getFileName(level + 1, 1).c_str());
      this->levels += 1;
      this->numFileAtLevel.push_back(1);
    } else if (this->numFileAtLevel[level+1] != 0){
      std::rename((this->DIR + "/" + newFileName).c_str(), this->_getFileName(level + 1, ++this->numFileAtLevel[level+1]).c_str());
    } else{
      std::rename((this->DIR + "/" + newFileName).c_str(), this->_getFileName(level + 1, ++this->numFileAtLevel[level+1]).c_str());
      this->numFileAtLevel[level+1] = 1;
    }
    this->bloomfilters[{level + 1, this->numFileAtLevel[level + 1]}] = newBF;
    this->fencePointers[{level + 1, this->numFileAtLevel[level + 1]}] = newFP;


    for (auto file : files){
      this->files[{file->level, file->k}] = nullptr;
      std::remove(file->name.c_str());
      delete file;
    }
  }
}

// leveling
Value LSM::_LevelingGet(int key){
  // search input buffer
  Value bufferValue = this->inputBuffer.find(key);
  if (bufferValue.valid == true){
    return bufferValue;
  }

  Value result;
  for (int i = 1; i <= levels; ++i){
    FileIdentifier fileChecking = FileIdentifier(i,1);

    auto bloomFilterElem = this->bloomfilters[fileChecking];
    if (bloomFilterElem != nullptr && bloomFilterElem->query(std::to_string(key))){
      FencePointers* fencePointer = this->fencePointers[fileChecking];
      LSMFile* file = this->_getFile(i, 1);

      std::pair<streampos, streampos> fence = fencePointer->getOffset(key); // get the fence region
      streampos trav = fence.first;

      // find file end
      file->file.seekg(0, std::ios::end);
      streampos end = file->file.tellg();
      streampos last = (std::streamoff) fence.second != -1 ? fence.second : end; // check whether the right end of the fence is at the end of the file;

      // start traverse the fence
      file->resetIndex(trav);
      Tuple t;
      do {
        file->loadTuple(&t);
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
  if (this->inputBuffer.size() + val.getTupleSize() > PAGE_SIZE){
    this->_LevelingPushLevel(0);
  }
  this->inputBuffer.insert({key, val});
}

std::vector<Value> LSM::_LevelingScan(){
  std::unordered_set<int> keyFound;
  std::vector<Value> result;
  Tuple t;

  // scan the input buffer first
  auto inputBufferIter = this->inputBuffer.cbegin();
  while(inputBufferIter != this->inputBuffer.cend()){
    if (keyFound.find(inputBufferIter->first) == keyFound.cend()){ // if it is a new key
      result.push_back(inputBufferIter->second);
      keyFound.insert(inputBufferIter->first);
    }
    ++inputBufferIter;
  }

  // scan the files
  for (int i = 1; i <= this->levels; ++i){
    FileIdentifier fileChecking = FileIdentifier(i,1);
    LSMFile* file = this->_getFile(i, 1);

    file->file.seekg(0, std::ios::end);
    streampos end = file->file.tellg();

    file->resetIndex(0);
    int numTuples;
    file->file.read((char*)&numTuples, LSM_PAGE_HEADER_SIZE);
    streampos trav = file->file.tellg();

    int count = 0;
    while (trav < end && count < numTuples){
      int gcount = file->loadTuple(&t);
      ++count;
      trav = file->file.tellg();
      if (keyFound.find(t.getKey()) == keyFound.cend()){ // if it is a new key
        Value newValue;
        t.dumpValue(&newValue);
        if (newValue.valid) {
          result.push_back(newValue);
          keyFound.insert(inputBufferIter->first);
        }
      }
    }
  }

  return result;
}

std::vector<Value> LSM::_LevelingScan(int min_key, int max_key){
  std::unordered_set<int> keyFound;
  std::vector<Value> result;
  Tuple t;

  // scan the input buffer first
  auto inputBufferIter = this->inputBuffer.cbegin();
  while(inputBufferIter != this->inputBuffer.cend()){
    if (keyFound.find(inputBufferIter->first) == keyFound.cend() && inputBufferIter->first >= min_key && inputBufferIter->first <= max_key){ // if it is a new key
      result.push_back(inputBufferIter->second);
      keyFound.insert(inputBufferIter->first);
    }
    ++inputBufferIter;
  }

  // scan the files
  for (int i = 1; i <= this->levels; ++i){
    FileIdentifier fileChecking = FileIdentifier(i,1);
    FencePointers* fp = (this->fencePointers.find(fileChecking))->second;
    std::pair<streampos,streampos> minPair = fp->getOffset(min_key);
    std::pair<streampos,streampos> maxPair = fp->getOffset(max_key);
    LSMFile* file = this->_getFile(i, 1);

    streampos trav = minPair.first;
    file->file.seekg(0, std::ios::end);
    streampos end = (std::streamoff) maxPair.second != -1 ? maxPair.second : file->file.end; // check whether the right end of the fence is at the end of the file;

    if ((std::streamoff) trav == -1) {continue;}// min less than the minimum in the file
    
    int prevKey = INT32_MIN;
    file->resetIndex(trav);
    while (trav <= end){
      file->loadTuple(&t);
      trav = file->file.tellg();
      if (keyFound.find(t.getKey()) == keyFound.cend() && t.getKey() >= min_key && t.getKey() <= max_key && t.getKey() > prevKey){ // if it is a new key
        Value newValue;
        t.dumpValue(&newValue);
        if (newValue.valid) {
          result.push_back(newValue);
          keyFound.insert(inputBufferIter->first);
        }
      }
      prevKey = t.getKey();
    }
  }

  return result;
}

void LSM::_LevelingPushLevel(int level){
  if (this->levels > level && this->numFileAtLevel[level + 1] == this->_LevelFileLimit(level + 1)){
      this->_LevelingPushLevel(level + 1);
  }
  if (level == 0){

    std::string tempFileName = this->writeBufferFile();

    if (this->numFileAtLevel.size() < 2){
      std::rename(tempFileName.c_str(), this->_getFileName(level + 1, 1).c_str());
      this->levels += 1;
      this->numFileAtLevel.push_back(1);
    } else if (this->numFileAtLevel[level+1] != 0){
      std::vector<LSMFile*> files;
      files.push_back(new LSMFile(0,1, tempFileName));
      this->_levelingMerge(level, files);
    } else{
      std::rename(tempFileName.c_str(), this->_getFileName(level + 1, 1).c_str());
      this->numFileAtLevel[1] = 1;
    }
  } else{
    std::vector<LSMFile*> files;
    this->_levelingMerge(level, files);
  }
    
}

Value LSM::get(int key){
  switch (MODE){
    case TIERING:
    return this->_TieringGet(key);
    case LEVELING:
    return this->_LevelingGet(key);
  }
}
void LSM::put(int key, Value val){
  switch (MODE){
    case TIERING:
    return this->_TieringPut(key, val);
    case LEVELING:
    return this->_LevelingPut(key, val);
  }
}
std::vector<Value> LSM::scan(){
  switch (MODE){
    case TIERING:
    return this->_TieringScan();
    case LEVELING:
    return this->_LevelingScan();
  }
}
std::vector<Value> LSM::scan(int min_key, int max_key){
  switch (MODE){
    case TIERING:
    return this->_TieringScan(min_key, max_key);
    case LEVELING:
    return this->_LevelingScan(min_key, max_key);
  }
}