//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_FUNCTION_SUM_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_FUNCTION_SUM_H

#include <string>
#include <unordered_map>

#include <normal/tuple/TupleSet.h>

namespace normal::pushdown::aggregate {

/**
 * Structure for aggregation functions to store intermediate results.
 *
 * It is intended for there to be one of these per aggregate function.
 *
 * This is roughly equivalent to a map, so aggregate functions can store multiple values, before
 * computing the final result. E.g. Mean most accurately calculated by storing count and sum before computing
 * the final result.
 */
class AggregationResult {

public:
  AggregationResult();

  void put(const std::string &key, const std::shared_ptr<arrow::Scalar> &value);
  std::shared_ptr<arrow::Scalar> get(const std::string &key);
  std::shared_ptr<arrow::Scalar> get(const std::string &key, const std::shared_ptr<arrow::Scalar> &defaultValue);
  void finalize(const std::shared_ptr<arrow::Scalar> &value);
  std::shared_ptr<arrow::Scalar> evaluate();
  void reset();

private:
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>>> result_;
  std::shared_ptr<arrow::Scalar> resultFinal_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_FUNCTION_SUM_H
