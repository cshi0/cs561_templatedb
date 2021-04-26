#include "BloomFilter.h"
#include "murmurhash.h"

using namespace std;
using namespace BF;

BloomFilter::BloomFilter(){
	numElement = 1024;
	bitsPerElement = 10;

	numIndex = (int)floor(0.693*bitsPerElement+ 0.5);
	size = numElement * bitsPerElement;
	makeBloomFilter();
}

BloomFilter::BloomFilter( int numElement_, int bitsPerElement_ ){
	numElement = numElement_;
	bitsPerElement = bitsPerElement_;
	numIndex = (int)floor(0.693*bitsPerElement+ 0.5);
	size = numElement * bitsPerElement;

	makeBloomFilter();
}

void BloomFilter::makeBloomFilter(){
	bf_vec.resize(size, 0);
}

void BloomFilter::program( string key ){
	vector<int> index( numIndex, 0 );
	getIndex( key, &index );

	for(int i=0; i<numIndex; i++){
		bf_vec[index[i]] = 1;
	}
}

bool BloomFilter::query( string key ){
	vector<int> index( numIndex, 0 );
	getIndex( key, &index );

	for(int i=0; i<numIndex; i++){
		if( bf_vec[index[i]] == 0 )
			return false;
	}

	return true; // positive
}

void BloomFilter::getIndex( string key, vector<int>* index ){
	unsigned int h = MurmurHash2( key.c_str(), key.size(), 0xbc9f1d34 );

	const unsigned int delta = (h>>17) | (h<<15); // Rorate right 17 bits
	for( int i=0 ; i<numIndex ; i++ ){
		index->at(i) = h % size;
		h += delta;
	}

	return;
}

int BloomFilter::getIndexNum(){
	return numIndex;
}

int BloomFilter::getSize(){
	return size;
}

void BloomFilter::serialize(std::ofstream& file){
	file.write((char*)&this->bitsPerElement, sizeof(int));
	file.write((char*)&this->numElement, sizeof(int));

	size_t size = bf_vec.size();
	file.write((char*)&size, sizeof(size_t));

	for (int i = 0; i < size; ++i){
		bool bit = bf_vec[i];
		file.write((char*)&bit, sizeof(bool));
	}
}

bool BloomFilter::deserialize(std::fstream& file){
	int _bitsPerElement, _numElement;
	file.read((char*)&_bitsPerElement, sizeof(int));
	file.read((char*)&_numElement, sizeof(int));

	if (file.gcount() == 0) {return false;}

	this->bitsPerElement = _bitsPerElement;
	this->numElement = _numElement;
	numIndex = (int)floor(0.693*bitsPerElement+ 0.5);
	size = numElement * bitsPerElement;
	makeBloomFilter();

	size_t size; 
	file.read((char*)&size, sizeof(size_t));
	for (int i = 0; i < size; ++i){
		bool bit;
		file.read((char*)&bit, sizeof(bool));
		bf_vec[i] = bit;
	}
	return true;
}

int BloomFilter::count(){
	return std::count(bf_vec.begin(), bf_vec.end(), true);
}