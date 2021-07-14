//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SUMLOGICALFUNCTION_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SUMLOGICALFUNCTION_H

#include <memory>

#include <normal/pushdown/aggregate/AggregationFunction.h>
#include <normal/plan/function/AggregateLogicalFunction.h>

namespace normal::plan::function {

class SumLogicalFunction : public AggregateLogicalFunction {

public:
  explicit SumLogicalFunction();

  std::shared_ptr<pushdown::aggregate::AggregationFunction> toExecutorFunction() override;

  std::shared_ptr<pushdown::aggregate::AggregationFunction> toExecutorReduceFunction() override;
};

}


#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SUMLOGICALFUNCTION_H
