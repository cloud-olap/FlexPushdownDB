//
// Created by matt on 5/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_SLICEDBLOOMFILTER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_SLICEDBLOOMFILTER_H

#include <memory>
#include <vector>
#include <cmath>

#include "UniversalHashFunction.h"

class SlicedBloomFilter {
public:
  SlicedBloomFilter(int capacity, double falsePositiveRate);
  static std::shared_ptr<SlicedBloomFilter> make(long capacity, double falsePositiveRate);

  bool add(int key);
  bool contains(int key);
  [[nodiscard]] size_t size() const;

  /**
   * Calculates the achievable false positive rate for the given number of bits and capacity of the bloom filter
   * @param bits
   * @param capacity
   * @return
   */
  static double calculateFalsePositiveRate(int bits, int capacity);

private:
  int capacity_;
  double falsePositiveRate_;

  int numSlices_;
  int numBitsPerSlice_;
  int numBits_;
  int count_;

  std::vector<std::shared_ptr<UniversalHashFunction>> hashFunctions_;
  std::vector<std::vector<bool>> bitArrays_;

  [[nodiscard]] int calculateNumBitsPerSlice() const;
  [[nodiscard]] int calculateNumSlices() const;
  [[nodiscard]] int calculateNumBits() const;

  [[nodiscard]] std::vector<std::shared_ptr<UniversalHashFunction>> makeHashFunctions() const;
  [[nodiscard]] std::vector<std::vector<bool>> makeBitArrays() const;

  std::vector<bool> hashes(int key);

  /**
   * Conversion formulas below
   *
   * n Capacity
   * p False positive rate
   * k Number of slices
   * o Bits per slice
   * m Number of bits
   * r Ratio of bits to capacity
   */

  /**
   *
   * @param n Capacity
   * @param p False positive rate
   * @param k Number of slices
   * @return o Bits per slice
   */
  static int o_from_npk(int n, double p, int k);

  /**
   *
   * @param m Number of bits
   * @param n Capacity
   * @return
   */
  static double p_from_mn(int m, int n);

  /**
   *
   * @param m
   * @param n
   * @return
   */
  static int k_from_mn(int m, int n);

  /**
   *
   * @param p False positive rate
   * @return k Number of slices
   */
  static int k_from_p(double p);

  /**
   *
   * @param m Number of bits
   * @param n Capacity
   * @return r Ratio of bits to capacity
   */
  static double r_from_mn(int m, int n);

  /**
   *
   * @param k Number of slices
   * @param r ???
   * @return p False positive rate
   */
  static int p_from_kr(int k, double r);

  /***
   *
   * @param k Number of slices
   * @param m Number of bits
   * @param n Capacity
   * @return p False positive rate
   */
  static double p_from_kmn(int k, int m, int n);

  /**
   *
   * @param r
   * @return k Number of slices
   */
  static int k_from_r(double r);

  /**
   *
   * @param k Number of slices
   * @param o Bits per slice
   * @return Number of bits
   */
  static int m_from_ko(int k, int o);

  /**
   *
   * @param m Number of bits
   * @param n Capacity
   * @return kp (Number of slices, False positive rate)
   */
  static std::pair<int, double> kp_from_mn(int m, int n);

  /**
   *
   * @param n Capacity
   * @param p False positive rate
   * @return k Number of slices
   */
  static int k_from_np(int n, double p);

};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_SLICEDBLOOMFILTER_H
