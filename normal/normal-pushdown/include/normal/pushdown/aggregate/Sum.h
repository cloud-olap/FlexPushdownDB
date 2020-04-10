//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_SUM_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_SUM_H

#include <normal/pushdown/aggregate/AggregationFunction.h>

namespace normal::pushdown::aggregate {

class Sum : public AggregationFunction {

private:
  std::shared_ptr<normal::core::expression::Expression> expression_;

public:
  Sum(std::string columnName, std::shared_ptr<normal::core::expression::Expression> expression);
  ~Sum() override = default;

  void apply(std::shared_ptr<normal::core::TupleSet> tuples) override;
  std::shared_ptr<arrow::DataType> returnType() override;
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_SUM_H
