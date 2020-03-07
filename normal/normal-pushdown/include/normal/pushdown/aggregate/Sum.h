//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_SUM_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_SUM_H

#include <normal/pushdown/aggregate/AggregationFunction.h>

namespace normal::pushdown::aggregate {

class Sum : public AggregationFunction {

private:
  std::string inputColumnName_;

public:
  Sum(std::string columnName, std::string inputColumnName);
  ~Sum() override = default;

  [[nodiscard]] const std::string &inputColumnName() const;

  void apply(std::shared_ptr<normal::core::TupleSet> tuples) override;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_SUM_H
