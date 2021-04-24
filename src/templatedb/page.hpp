#ifndef __TEMPLATEDB_PAGE_H_
#define __TEMPLATEDB_PAGE_H_

#include<fstream>
#include<iterator>
#include<vector>
#include<map>
#include <stdio.h>
#include <cstring>

#include "BloomFilter/BloomFilter.h"

#ifndef PAGE_SIZE
#define PAGE_SIZE 128
#define FENCEPOINTER_GRANULARITY 5
#endif

#define LSM_PAGE_HEADER_SIZE sizeof(int)
#define TUPLE_VALID_BYTE_SIZE 1
#define TUPLE_KEY_SIZE sizeof(int)
#define TUPLE_VALID_BYTE_OFFSET 0
#define TUPLE_KEY_OFFSET 1
#define TUPLE_VALUE_OFFSET (TUPLE_VALID_BYTE_SIZE + TUPLE_KEY_SIZE)
#define MSB_TO_LSB 7
#define LSB_MASK 0x1
#define VALID_MASK 0x80 // set the first bit
#define DIM_MASK 0x7F // dimension mask: 0111 1111

#define MAX_TUPLE_SIZE 128

namespace templatedb
{

  static int LARGEST_KEY_VALUE_SIZE = 0;

  class FencePointers{
    std::vector<std::pair<streampos, int>> minValues; // (file position, key)
    
    public:
      std::pair<streampos, streampos> getOffset(int key){
        if (minValues[0].second > key){
          return std::pair<streampos, streampos>(streampos(-1), streampos(-1));
        }
        for (int i = 0; i < minValues.size(); ++i){
          if (minValues[i].second > key){
            return std::pair<streampos,streampos>(minValues[i-1].first, minValues[i].first);
          }
        }
        return std::pair<streampos,streampos>(minValues.back().first, streampos(-1));
      }

      inline void add(streampos index, int key){
        this->minValues.push_back(std::pair<streampos, int>(index, key));
      }
  }; /* FencePointers */

  class Value{
  public:
    std::vector<int> items;
    bool visible = true;
    bool valid = false;

    Value() {valid = false;}
    Value(bool _visible) {visible = _visible; valid = true;}
    Value(std::vector<int> _items) { items = _items; valid = true;}

    inline size_t getTupleSize(){
      return items.size() * sizeof(int) + TUPLE_KEY_SIZE + TUPLE_VALID_BYTE_SIZE;
    }

    bool operator ==(Value const & other) const
    {
        return (visible == other.visible) && (items == other.items);
    }
  }; /* Value */

  class InputBuffer{
  public:
    std::map<int, Value> inputBuffer;
    size_t byte_size;

    inline void clear(){
      this->inputBuffer.clear();
    }

    inline void insert(std::pair<int, Value> kv){
      this->inputBuffer[kv.first] = kv.second;
      byte_size += kv.second.getTupleSize();
    }

    inline Value find(int key){
      return this->inputBuffer[key];
    }

    inline auto cbegin(){
      return this->inputBuffer.cbegin();
    }
    inline auto cend(){
      return this->inputBuffer.cend();
    }
    inline size_t size(){
      return this->byte_size;
    }
  };

  class Tuple{
    private:
      inline void setValidBit(bool isValid){
        if (isValid){
          buffer[0] |= VALID_MASK;
        } else{
          buffer[0] &= DIM_MASK;
        }
      }

      void loadDimension(int size){
        if (size > 0x80){ std::cerr << "dimension is larger than 127" << std::endl; }
        if (size > LARGEST_KEY_VALUE_SIZE){
          LARGEST_KEY_VALUE_SIZE = size;
          BUFFER_SIZE = LARGEST_KEY_VALUE_SIZE * sizeof(int) + 1;
          free(this->buffer);
          buffer = (char*)malloc(BUFFER_SIZE);
        }
        buffer[0] = 0;
        buffer[0] |= VALID_MASK;
        buffer[0] |= (char)(size - 1);
      }

    public:
      int BUFFER_SIZE;

      char* buffer;

      Tuple() {
        BUFFER_SIZE = LARGEST_KEY_VALUE_SIZE * sizeof(int) + 1;
        buffer = (char*)malloc(BUFFER_SIZE);
      }
      ~Tuple(){free(buffer);}
      
      inline bool isValid(){
        return (buffer[TUPLE_VALID_BYTE_OFFSET] >> MSB_TO_LSB) & LSB_MASK;
      }
      inline int getDimension(){
        return (buffer[TUPLE_VALID_BYTE_OFFSET] & DIM_MASK);
      }
      inline int getKey() {
        return *reinterpret_cast<int*>(&buffer[TUPLE_KEY_OFFSET]);
      }

      inline int getActualSize(){
        return (this->getDimension() * sizeof(int)) + TUPLE_KEY_SIZE + TUPLE_VALID_BYTE_SIZE;
      }

      void dumpValue(Value* result){
        result->valid = true;
        result->visible = this->isValid();
        int* values = reinterpret_cast<int*>(&buffer[TUPLE_VALUE_OFFSET]);

        result->items.clear();
        for (int i = 0; i < this->getDimension(); ++i){
          result->items.push_back(values[i]);
        }
      }

      void loadFromArray(bool isValid, int* array, int size){
        memset(this->buffer, 0, BUFFER_SIZE);
        this->loadDimension(size);

        this->setValidBit(isValid);

        int* numP = (int*)(&buffer[TUPLE_KEY_OFFSET]);
        for (int i = 0; i < size; ++i){
          *numP++ = *array++;
        }
      }

      inline void writeToFile(std::fstream* file){
        int size = this->getDimension()*sizeof(int) + TUPLE_KEY_SIZE + TUPLE_VALID_BYTE_SIZE;
        file->write(buffer, size);
      }

      inline void writeToFile(std::ofstream* file){
        int size = this->getDimension()*sizeof(int) + TUPLE_KEY_SIZE + TUPLE_VALID_BYTE_SIZE;
        file->write(buffer, size);
      }

      int readFromFile(std::fstream& file){
        memset(this->buffer, 0, BUFFER_SIZE);
        int gcount = 0;
        file.read(buffer, TUPLE_VALID_BYTE_SIZE);
        gcount += file.gcount();
        int dimension = this->getDimension();

        this->loadDimension(dimension + 1);
        file.read(buffer+TUPLE_KEY_OFFSET, TUPLE_KEY_SIZE);
        gcount += file.gcount();
        char* bufferPtr = buffer+TUPLE_VALID_BYTE_SIZE+TUPLE_KEY_SIZE;
        for (int i = 0; i < dimension; ++i){
          file.read(bufferPtr, sizeof(int));
          gcount += file.gcount();
          bufferPtr += sizeof(int);
        }
        return gcount;
      }
  }; /* Tuple */

  class LSMFile{
    public:
      unsigned tupleNum;

      unsigned level;
      unsigned k;
      std::string name;
      std::fstream file;

      LSMFile(unsigned _level, unsigned _k, const std::string & _name);
      ~LSMFile(){file.close();}

      inline bool good() {return this->file.is_open() && this->file.good();}
      inline void resetIndex(streampos idx);

      int loadTuple(Tuple* tuple);
      int loadTuple(Tuple* tuple, streampos idx);
  };

  void mergeFiles(std::vector<LSMFile*> & files, const std::string & dir, std::string & newFileName, FencePointers* fp, BF::BloomFilter* bf);

} // namespace templatedb


#endif