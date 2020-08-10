//
// Created by matt on 5/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_UNIVERSALHASHFUNCTION_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_UNIVERSALHASHFUNCTION_H

#include <memory>

/**
 * Class implementing universal family of hash function (Carter and Wegman)
 *
 * h(x) = ((ax + b) mod p) mod m
 *
 * All variables are stored as longs to avoid integer overflow when calculating the hash.
 *
 * TODO: May need to template this to allow hashing variable width integers?
 */
class UniversalHashFunction {

public:

  /**
   * Creates a hash function hashing keys (x) into the given number of bins (m)
   * @param m
   */
  explicit UniversalHashFunction(int m);
  [[nodiscard]] static std::shared_ptr<UniversalHashFunction> make(int m);

  /**
   * Hashes the given key (x)
   * @param x
   * @return
   */
  [[nodiscard]] int hash(int x) const;

protected:
  long a_;
  long b_;
  long m_;
  long p_;

};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_UNIVERSALHASHFUNCTION_H
