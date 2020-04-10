//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_AGGREGATEEXPRESSION2_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_AGGREGATEEXPRESSION2_H

#include <memory>
#include <normal/core/TupleSet.h>
#include "AggregationResult.h"

namespace normal::pushdown::aggregate {

/**
 * Base class for aggregation functions
 */
class AggregationFunction {

private:
  std::string columnName_;
  std::shared_ptr<aggregate::AggregationResult> buffer_;

protected:
  std::shared_ptr<aggregate::AggregationResult> result();

public:
  explicit AggregationFunction(std::string columnName);
  void init(std::shared_ptr<aggregate::AggregationResult> result);
  [[nodiscard]] const std::string &columnName() const;

  virtual ~AggregationFunction() = 0;
  virtual void apply(std::shared_ptr<normal::core::TupleSet> tuples) = 0;
  virtual std::shared_ptr<arrow::DataType> returnType() = 0;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_AGGREGATEEXPRESSION2_H
