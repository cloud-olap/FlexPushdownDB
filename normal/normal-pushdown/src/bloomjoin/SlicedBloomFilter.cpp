//
// Created by matt on 5/8/20.
//

#include <cmath>
#include <cassert>

#include "normal/pushdown/bloomjoin/SlicedBloomFilter.h"

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
  assert(falsePositiveRate >= 0.0 && falsePositiveRate <= 1.0);
}

std::shared_ptr<SlicedBloomFilter> SlicedBloomFilter::make(long capacity, double falsePositiveRate) {
  return std::make_shared<SlicedBloomFilter>(capacity, falsePositiveRate);
}

int SlicedBloomFilter::calculateNumSlices() const {
  auto k = k_from_p(falsePositiveRate_);
  assert(k > 0);
  return k;
}

int SlicedBloomFilter::calculateNumBitsPerSlice() const {
  auto o = o_from_npk(capacity_, falsePositiveRate_, numSlices_);
  assert(o > 0);
  return o;
}

int SlicedBloomFilter::calculateNumBits() const {
  auto m = m_from_ko(numSlices_, numBitsPerSlice_);
  assert(m > 0);
  return m;
}

double SlicedBloomFilter::calculateFalsePositiveRate(int bits, int capacity) {
  auto p = p_from_mn(bits, capacity);
  assert(p >= 0.0 && p <= 1.0);
  return p;
}

std::vector<std::vector<bool>> SlicedBloomFilter::makeBitArrays() const {
  return std::vector<std::vector<bool>>(numSlices_, std::vector<bool>(numBitsPerSlice_, false));
}

std::vector<std::shared_ptr<UniversalHashFunction>> SlicedBloomFilter::makeHashFunctions() const {
  std::vector<std::shared_ptr<UniversalHashFunction>> hashFunctions;
  hashFunctions.reserve(numSlices_);
  for (int s = 0; s < numSlices_; ++s) {
	hashFunctions.emplace_back(UniversalHashFunction::make(numBits_));
  }
  return hashFunctions;
}

bool SlicedBloomFilter::add(int key) {

  auto hs = hashes(key);
  bool foundAllBits = true;

  for (size_t i = 0; i < hs.size(); ++i) {
	if (foundAllBits && not bitArrays_[i][hs[i]]) {
	  foundAllBits = false;
	}
	bitArrays_[i][hs[i]] = true;
  }

  if (!foundAllBits) {
	++count_;
	return false;
  } else {
	return true;
  }
}

std::vector<bool> SlicedBloomFilter::hashes(int key) {

  std::vector<bool> hashes(hashFunctions_.size());

  for (size_t i = 0; i < hashFunctions_.size(); ++i) {
	auto h = hashFunctions_[i]->hash(key);
	hashes[i] = bitArrays_[i][h];
  }

  return hashes;
}

bool SlicedBloomFilter::contains(int key) {

  auto hs = hashes(key);

  for (size_t i = 0; i < hs.size(); ++i) {
	if (!bitArrays_[i][hs[i]]) {
	  return false;
	}
  }

  return true;
}

size_t SlicedBloomFilter::size() const {
  return count_;
}

int SlicedBloomFilter::o_from_npk(int n, double p, int k) {

  assert(n > 0);

  if (k == 0) {
	return 0;
  } else {
	return std::ceil(
		(n * std::abs(std::log(p))) /
			(k * (std::pow(std::log(2), 2))));
  }
}

int SlicedBloomFilter::k_from_p(double p) {
  return std::ceil(std::log2(1.0 / p));
}

double SlicedBloomFilter::r_from_mn(int m, int n) {

  assert(n > 0);

  return static_cast<double>(m) / static_cast<double>(n);
}

int SlicedBloomFilter::p_from_kr(int k, double r) {
  return std::pow(1 - std::exp(static_cast<double>(0 - k) / r), k);
}

double SlicedBloomFilter::p_from_kmn(int k, int m, int n) {

  assert(n > 0);

  double r = SlicedBloomFilter::r_from_mn(m, n);
  return SlicedBloomFilter::p_from_kr(k, r);
}

int SlicedBloomFilter::k_from_r(double r) {
  return static_cast<int>(std::round(std::log(2) * r));
}

int SlicedBloomFilter::m_from_ko(int k, int o) {
  return k * o;
}

std::pair<int, double> SlicedBloomFilter::kp_from_mn(int m, int n) {

  assert(n > 0);

  double r = SlicedBloomFilter::r_from_mn(m, n);
  int k = SlicedBloomFilter::k_from_r(r);
  int p = SlicedBloomFilter::p_from_kr(k, r);
  return std::pair(k, p);
}

double SlicedBloomFilter::p_from_mn(int m, int n) {

  assert(n > 0);

  double r = SlicedBloomFilter::r_from_mn(m, n);
  int k = SlicedBloomFilter::k_from_r(r);
  double p = SlicedBloomFilter::p_from_kr(k, r);
  return p;
}
