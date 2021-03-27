#ifndef __TEMPLATEDB_PAGE_H_
#define __TEMPLATEDB_PAGE_H_

#include<fstream>
#include<iterator>
#include<vector>

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

#define LSM_PAGE_HEADER_SIZE 2*sizeof(int)
#define TUPLE_VALID_BYTE_SIZE 1
#define TUPLE_KEY_SIZE sizeof(int)
#define TUPLE_VALID_BYTE_OFFSET 0
#define TUPLE_KEY_OFFSET 1
#define TUPLE_VALUE_OFFSET (TUPLE_VALID_BYTE_SIZE + TUPLE_KEY_SIZE)

namespace templatedb
{

  class FencePointers{
    std::vector<std::pair<int, int>> minValues; // (tuple index, key)
    
    public:
      std::pair<int, int> getOffset(int key){
        for (int i = 0; i < minValues.size(); ++i){
          if (minValues[i].second > key){
            return std::pair<int,int>(minValues[i-1].first, minValues[i].first);
          }
          return std::pair<int,int>(minValues.back().first, INT32_MAX);
        }
      }

      inline void add(int index, int key){
        this->minValues.push_back(std::pair<int, int>(index, key));
      }
  }; /* FencePointers */

  class Value{
  public:
    std::vector<int> items;
    bool visible = true;

    Value() {}
    Value(bool _visible) {visible = _visible;}
    Value(std::vector<int> _items) { items = _items;}

    bool operator ==(Value const & other) const
    {
        return (visible == other.visible) && (items == other.items);
    }
  }; /* Value */

  class Tuple{
    public:
      const unsigned dimension;
      const int TUPLE_SIZE;
      char* buffer;

      Tuple(int _dimension) : dimension(_dimension), TUPLE_SIZE(TUPLE_VALID_BYTE_SIZE + TUPLE_KEY_SIZE + (_dimension * sizeof(int))) {
        buffer = (char*)malloc(TUPLE_SIZE);
      }
      ~Tuple(){free(buffer);}
      
      inline bool isValid(){
        return buffer[TUPLE_VALID_BYTE_OFFSET];
      }
      inline int getKey() {
        return *reinterpret_cast<int*>(&buffer[TUPLE_KEY_OFFSET]);
      }

      void dumpValue(Value* result){
        result->visible = this->isValid();
        int* values = reinterpret_cast<int*>(&buffer[TUPLE_VALUE_OFFSET]);
        for (int i = 0; i < this->dimension; ++i){
          result->items.push_back(values[i]);
        }
      }

      void loadFromArray(bool isValid, int* array, int size){
        if (isValid){
          buffer[0] = 1;
        } else{
          buffer[0] = 0;
        }

        int* numP = (int*)(&buffer[TUPLE_KEY_OFFSET]);
        for (int i = 0; i < size; ++i){
          *numP++ = *array++;
        }
      }

      inline void writeToFile(std::fstream* file){
        file->write(buffer, TUPLE_SIZE);
      }

      inline void writeToFile(std::ofstream* file){
        file->write(buffer, TUPLE_SIZE);
      }
  }; /* Tuple */

  class LSMFile{
    public:
      unsigned dimension;
      unsigned tupleNum;
      unsigned tupleSize;

      unsigned currIdx;

      unsigned level;
      unsigned k;
      std::string name;
      std::fstream file;

      LSMFile(unsigned _level, unsigned _k, std::string & _name);
      ~LSMFile(){file.close();}


      inline bool good() {return this->file.is_open() && this->file.good();}
      inline void resetIndex(int idx);


      int loadTuple(Tuple* tuple);
      int loadTuple(Tuple* tuple, int idx);
  };

  void mergeFiles(std::vector<LSMFile*> & files, const std::string & dir, std::string & newFileName, FencePointers* fp, BF::BloomFilter* bf);

} // namespace templatedb


#endif