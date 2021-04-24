#ifndef _TEMPLATEDB_LSM_H_
#define _TEMPLATEDB_LSM_H_

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <sstream>
#include <map>
#include <unordered_map>
#include <vector>

#include "templatedb/page.hpp"

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

#ifndef _LSM_CONSTANTS_
#define _LSM_CONSTANTS_

#define LSM_BUFFER_PAGE_NUM 10 // the input buffer contains 10 buffer pages
#define LSM_MERGE_RATIO 2 // default merge ratio is 2
#define LSM_FILE_NAME_PREFIX "LSM_STORAGE_"
#define LSM_META_DATA "LSM_META_DATA"

#endif

namespace templatedb{
  typedef std::pair<int, int> FileIdentifier;

  struct pair_hash { // https://stackoverflow.com/questions/32685540/why-cant-i-compile-an-unordered-map-with-a-pair-as-key
    template <class T1, class T2>
    std::size_t operator () (const std::pair<T1,T2> &p) const {
        auto h1 = std::hash<T1>{}(p.first);
        auto h2 = std::hash<T2>{}(p.second);
        return h1 ^= h2 + 0x9e3779b9 + (h1<<6) + (h1>>2); // the hash combine function used in boost
    }
  };

  typedef enum _status_code{
    OPEN = 0,
    CLOSED = 1,
    ERROR_OPEN = 100,
  } db_status;

  typedef enum _lsm_mode_code{
    TIERING = 0,
    LEVELING = 1,
  } lsm_mode;


  class LSM{
    private:
      inline int _LevelFileLimit(int level) {return std::pow(LSM_MERGE_RATIO, level);}
      inline std::string _getFileName(int level, int k) {return DIR + "/" + LSM_FILE_NAME_PREFIX + std::to_string(level) + "_" + std::to_string(k);}

      LSMFile* _getFile(int level, int k); // k start from 1

      void _levelingMerge(int level, std::vector<LSMFile*> files);

      Value _TieringGet(int key);
      void _TieringPut(int key, Value val);
      std::vector<Value> _TieringScan();
      std::vector<Value> _TieringScan(int min_key, int max_key);
      void _TieringPushLevel(int level);

      Value _LevelingGet(int key);
      void _LevelingPut(int key, Value val);
      std::vector<Value> _LevelingScan();
      std::vector<Value> _LevelingScan(int min_key, int max_key);
      void _LevelingPushLevel(int level);
    
    public:
      std::string DIR; // directory where the LSM tree resides
      lsm_mode MODE; // leveling or tiering

      std::unordered_map<FileIdentifier, LSMFile*, pair_hash> files;

      InputBuffer inputBuffer; // the input buffer is a red-black tree

      int levels = 0; // level 0 is the inputBuffer
      std::unordered_map<FileIdentifier, FencePointers*, pair_hash> fencePointers;
      std::unordered_map<FileIdentifier, BF::BloomFilter*, pair_hash> bloomfilters;
      std::vector<int> numFileAtLevel = {0};
      LSM(std::string _dir, lsm_mode _mode) : DIR(_dir), MODE(_mode){}
      ~LSM(){close();}

      void close(){
        for (auto i : files){
          delete i.second;
        }

        for (auto i : fencePointers){
          delete i.second;
        }

        for (auto i : bloomfilters){
          delete i.second;
        }
      }

      Value get(int key);
      void put(int key, Value val);
      std::vector<Value> scan();
      std::vector<Value> scan(int min_key, int max_key);

      std::string writeBufferFile();
  }; /* LSM */

} /* templatedb */


#endif /* _TEMPLATEDB_LSM_H */