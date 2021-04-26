#include <vector>
#include <string>
#include <math.h>
#include <fstream>
#include <bitset>
#include <algorithm>

using namespace std;

namespace BF{

class BloomFilter {
public:
	BloomFilter();
	BloomFilter( int numElement_, int bitsPerElement_ );
	int numElement;
	int bitsPerElement;

	int count();

	void serialize(std::ofstream& file);
	bool deserialize(std::fstream& file);

	void program(string key);
	bool query(string key);

	int getIndexNum();
	int getSize();
private:
	vector< bool > bf_vec;
	int numIndex;
	int size;
	
	void makeBloomFilter();
	void getIndex( string key, vector<int>* index );
};

} // namespace BF
