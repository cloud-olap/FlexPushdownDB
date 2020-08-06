//
// Created by matt on 5/8/20.
//

#include <cmath>
#include <cassert>
#include "SlicedBloomFilter.h"

SlicedBloomFilter::SlicedBloomFilter(int capacity, double falsePositiveRate) :
	capacity_(capacity),
	falsePositiveRate_(falsePositiveRate),
	numSlices_(calculateNumSlices()),
	numBitsPerSlice_(calculateNumBitsPerSlice()),
	numBits_(calculateNumBits()),
	count_(0),
	hashFunctions_(makeHashFunctions()),
	bitArrays_(makeBitArrays()) {

  assert(capacity > 0);
  assert(falsePositiveRate > 0.0 && falsePositiveRate < 1.0);
}

std::shared_ptr<SlicedBloomFilter> SlicedBloomFilter::make(long capacity, double falsePositiveRate) {
  return std::make_shared<SlicedBloomFilter>(capacity, falsePositiveRate);
}

int SlicedBloomFilter::calculateNumSlices() const {
  return k_from_p(falsePositiveRate_);
}

int SlicedBloomFilter::calculateNumBitsPerSlice() const {
  return o_from_npk(capacity_, falsePositiveRate_, numSlices_);
}

int SlicedBloomFilter::calculateNumBits() const {
  return m_from_ko(numSlices_, numBitsPerSlice_);
}

std::vector<std::vector<bool>> SlicedBloomFilter::makeBitArrays() const {
  std::vector<std::vector<bool>> bitArrays(numSlices_, std::vector<bool>(numBitsPerSlice_, false));
//  for (auto bitArray: bitArrays) {
//	bitArray = std::vector<bool>(numBitsPerSlice_, false);
//  }
  return bitArrays;
}

std::vector<std::shared_ptr<UniversalSQLHashFunction>> SlicedBloomFilter::makeHashFunctions() const {
  std::vector<std::shared_ptr<UniversalSQLHashFunction>> hashFunctions;
  hashFunctions.reserve(numSlices_);
  for (int s = 0; s < numSlices_; ++s) {
	hashFunctions.emplace_back(UniversalSQLHashFunction::make(numBits_));
  }
  return hashFunctions;
}

std::pair<int, double> SlicedBloomFilter::kp_from_mn(int m, int n) {
  double r = SlicedBloomFilter::r_from_mn(m, n);
  int k = SlicedBloomFilter::k_from_r(r);
  int p = SlicedBloomFilter::p_from_kr(k, r);
  return std::pair(k, p);
}

double SlicedBloomFilter::p_from_mn(int m, long n) {
  double r = SlicedBloomFilter::r_from_mn(m, n);
  int k = SlicedBloomFilter::k_from_r(r);
  int p = SlicedBloomFilter::p_from_kr(k, r);
  return p;
}

int SlicedBloomFilter::m_from_ko(int k, int o) {
  return k * o;
}

int SlicedBloomFilter::k_from_r(double r) {
  return int(std::round(std::log(2) * r));
}

double SlicedBloomFilter::p_from_kmn(int k, int m, int n) {
  double r = SlicedBloomFilter::r_from_mn(m, n);
  return SlicedBloomFilter::p_from_kr(k, r);
}

int SlicedBloomFilter::p_from_kr(int k, double r) {
  return std::pow(1 - std::exp((0 - k) / r), k);
}

double SlicedBloomFilter::r_from_mn(int m, int n) {
  return double(m) / double(n);
}

int SlicedBloomFilter::k_from_p(double p) {
  return int(std::ceil(std::log2(1.0 / p)));
}

int SlicedBloomFilter::o_from_npk(int n, double p, int k) {
  if (k == 0) {
	return 0;
  } else {
	return int(std::ceil((n * std::abs(std::log(p))) / (k * (std::pow(std::log(2), 2)))));
  }
}

bool SlicedBloomFilter::add(int key) {

  auto hs = hashes(key);
  bool foundAllBits = true;

  for(size_t i=0;i<hs.size();++i){
	if(foundAllBits && not bitArrays_[i][hs[i]]){
	  foundAllBits = false;
	}
	bitArrays_[i][hs[i]] = true;
  }

  if (!foundAllBits){
	++count_;
	return false;
  }
  else{
	return true;
  }
}

std::vector<bool> SlicedBloomFilter::hashes(int key) {

  std::vector<bool> hashes(hashFunctions_.size());

  for(size_t i=0;i<hashFunctions_.size();++i){
	hashes[i] = bitArrays_[i][hashFunctions_[i]->hash(key)];
  }

  return hashes;
}

bool SlicedBloomFilter::contains(int key) {

  auto hs = hashes(key);

  for(size_t i=0;i<hs.size();++i){
	if(!bitArrays_[i][hs[i]]){
	  return false;
	}
  }

  return true;
}

size_t SlicedBloomFilter::size() const {
  return count_;
}




