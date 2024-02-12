//
// Created by Yifei Yang on 3/17/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_UNIVERSALHASHFUNCTION_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_UNIVERSALHASHFUNCTION_H

#include <tl/expected.hpp>
#include <nlohmann/json.hpp>
#include <memory>

namespace fpdb::executor::physical::bloomfilter {

/**
 * Class implementing universal family of hash function (Carter and Wegman)
 *
 * h(x) = ((ax + b) mod p) mod m
 *
 * All variables are stored as longs to avoid integer overflow when calculating the hash.
 */
class UniversalHashFunction {

public:
  /**
   * Creates a hash function hashing keys (x) into the given number of bits (m)
   * @param m
   */
  explicit UniversalHashFunction(int64_t m);
  static std::shared_ptr<UniversalHashFunction> make(int64_t m);

  /**
   * Used when reconstructing at store during pushdown
   */
  explicit UniversalHashFunction(int64_t a, int64_t b, int64_t m, int64_t p);
  static std::shared_ptr<UniversalHashFunction> make(int64_t a, int64_t b, int64_t m, int64_t p);

  /**
   * Hashes the given key (x)
   * @param x
   * @return
   */
  int64_t hash(int64_t x) const;

  ::nlohmann::json toJson() const;
  static tl::expected<std::shared_ptr<UniversalHashFunction>, std::string> fromJson(const nlohmann::json &jObj);

private:
  int64_t a_;
  int64_t b_;
  int64_t m_;
  int64_t p_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_UNIVERSALHASHFUNCTION_H
