//
// Created by matt on 5/8/20.
//

#include "normal/pushdown/bloomjoin/UniversalHashFunction.h"

#include <random>
#include <cassert>

#include <primesieve.hpp>

UniversalHashFunction::UniversalHashFunction(int m) {

  assert(m > 0);

  std::random_device rd;
  std::mt19937 gen(rd());

  primesieve::set_num_threads(1);

  m_ = m;

  // Pick a random integer greater than or equal to m (upper bound of 2m)
  int i = std::uniform_int_distribution<int>(m_, 2 * m_)(gen);

  // Get the 1st prime greater than or equal to i
  p_ = primesieve::nth_prime(0, i);

  // Set a = random int (less than p) (where a != 0)
  a_ = std::uniform_int_distribution<int>(1, p_ - 1)(gen);

  // Set b = random int (less than p)
  b_ = std::uniform_int_distribution<int>(0, p_ - 1)(gen);
}

std::shared_ptr<UniversalHashFunction> UniversalHashFunction::make(int m) {
  return std::make_shared<UniversalHashFunction>(m);
}

int UniversalHashFunction::hash(int x) const {
  long h = ((a_ * static_cast<long>(x) + b_) % p_) % m_;
  assert(h >= 0 && h <= m_);
  return static_cast<int>(h);
}
