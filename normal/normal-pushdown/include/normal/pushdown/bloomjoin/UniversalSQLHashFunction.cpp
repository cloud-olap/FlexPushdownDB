//
// Created by matt on 5/8/20.
//

#include <random>

#include <primesieve.hpp>
#include <fmt/format.h>

#include "UniversalSQLHashFunction.h"

UniversalSQLHashFunction::UniversalSQLHashFunction(int m) {

  std::random_device rd;
  std::mt19937 gen(rd());

  m_ = m;

  // Pick a random integer between m and 2m
  std::uniform_int_distribution<int> distribution1(m, 2 * m);
  int i = distribution1(gen);

  //Get the nth prime greater than i
  p_ = primesieve::nth_prime(1, i);

  //Set a = random int (less than p) (where a != 0)
  std::uniform_int_distribution<int> distribution2(1, p_ - 1);
  a_ = distribution2(gen);

  // Set b = random int (less than p)
  std::uniform_int_distribution<int> distribution3(0, p_ - 1);
  b_ = distribution3(gen);
}

std::shared_ptr<UniversalSQLHashFunction> UniversalSQLHashFunction::make(int numBits) {
  return std::make_shared<UniversalSQLHashFunction>(numBits);
}

size_t UniversalSQLHashFunction::hash(int k) const {
  return ((a_ * k + b_) % p_) % m_;
}

std::string UniversalSQLHashFunction::sql(int x, int m) {
  return fmt::format("(({} * cast({} as int) + {}) % {}) % {}", a_, x, b_, p_, m);
}
