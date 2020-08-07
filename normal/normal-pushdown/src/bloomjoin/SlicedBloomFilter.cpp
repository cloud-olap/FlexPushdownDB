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

  assert(falsePositiveRate >= 0.0 && falsePositiveRate <= 1.0);
}

std::shared_ptr<SlicedBloomFilter> SlicedBloomFilter::make(long capacity, double falsePositiveRate) {
  return std::make_shared<SlicedBloomFilter>(capacity, falsePositiveRate);
}

int SlicedBloomFilter::calculateNumSlices() const {
  return k_from_np(capacity_, falsePositiveRate_);
}

int SlicedBloomFilter::calculateNumBitsPerSlice() const {
  return o_from_npk(capacity_, falsePositiveRate_, numSlices_);
}

int SlicedBloomFilter::calculateNumBits() const {
  return m_from_ko(numSlices_, numBitsPerSlice_);
}

double SlicedBloomFilter::calculateFalsePositiveRate(int bits, int capacity) {
  return p_from_mn(bits, capacity);
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

  assert(capacity_ > 0);

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

  if(capacity_ == 0)
    return false;

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

  int o;

	if (k == 0) {
	  o = 0;
	} else {
	  o = std::ceil(
		  (n * std::abs(std::log(p))) /
			  (k * (std::pow(std::log(2), 2))));
	}

  return o;
}

int SlicedBloomFilter::k_from_p(double p) {
  return std::ceil(std::log2(1.0 / p));
}

double SlicedBloomFilter::r_from_mn(int m, int n) {
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

  double r = SlicedBloomFilter::r_from_mn(m, n);
  int k = SlicedBloomFilter::k_from_r(r);
  int p = SlicedBloomFilter::p_from_kr(k, r);
  return std::pair(k, p);
}

int SlicedBloomFilter::k_from_mn(int m, int n) {

  double r = SlicedBloomFilter::r_from_mn(m, n);
  int k = SlicedBloomFilter::k_from_r(r);

  return k;
}

double SlicedBloomFilter::p_from_mn(int m, int n) {

  double p;

  if (n == 0) {
    // If capacity = 0, then all filter tests will be false with 100% accuracy
	p = 0.0;
  } else {
	double r = SlicedBloomFilter::r_from_mn(m, n);
	int k = SlicedBloomFilter::k_from_r(r);
	p = SlicedBloomFilter::p_from_kr(k, r);
  }

  assert(p >= 0.0 && p <= 1.0);

  return p;
}

int SlicedBloomFilter::k_from_np(int n, double p) {

  int k;

  if(n == 0) {
	k = 0;
  }
  else {
	auto m = std::ceil(n * std::log(p) / std::log(1 / std::pow(2, std::log(2))));
	auto r = r_from_mn(m, n);
	k = k_from_r(r);
  }

  return k;
}
