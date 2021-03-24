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

#include "BloomFilter/BloomFilter.h"
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
      const std::string DIR; // directory where the LSM tree resides
      const lsm_mode MODE; // leveling or tiering
      const int DIMENSION;
      const int BUFFER_PAIR_LIMIT; // max number of the key-value pairs can be in the input buffer

      std::unordered_map<std::pair<int, int>, std::fstream> files;

      std::map<int, Value> inputBuffer; // the input buffer is a red-black tree

      int levels; // level 0 is the inputBuffer
      std::unordered_map<std::pair<int, int>, FencePointers> fencePointers;
      std::unordered_map<int, BF::BloomFilter> bloomfilters;

      inline int _LevelFileLimit(int level) {return std::pow(LSM_MERGE_RATIO, level);}
      inline std::string _getFileName(int level, int k) {return DIR + "/" + LSM_FILE_NAME_PREFIX + std::to_string(level) + "_" + std::to_string(k);}

      std::fstream _openFile(int level, int k);

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
      ~LSM(){close();}

      db_status open();
      void close();

      Value get(int key);
      void put(int key, Value val);
      std::vector<Value> scan();
      std::vector<Value> scan(int min_key, int max_key);
  }; /* LSM */

} /* templatedb */


#endif /* _TEMPLATEDB_LSM_H */