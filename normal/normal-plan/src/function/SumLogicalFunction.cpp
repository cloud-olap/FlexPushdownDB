//
// Created by matt on 7/4/20.
//

#include <normal/plan/function/SumLogicalFunction.h>

#include <normal/pushdown/aggregate/Sum.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Column.h>

using namespace normal::plan;
using namespace normal::expression::gandiva;

normal::plan::function::SumLogicalFunction::SumLogicalFunction() : normal::plan::function::AggregateLogicalFunction("sum") {}

std::shared_ptr<normal::pushdown::aggregate::AggregationFunction> normal::plan::function::SumLogicalFunction::toExecutorFunction() {
  // FIXME: Defaulting name to sum-{expression.alias()}
  if (name_.empty()) {
    name_ = fmt::format("sum-{}", this->expression()->alias());
  }
  return std::make_shared<normal::pushdown::aggregate::Sum>(name_, this->expression());
}

std::shared_ptr<normal::pushdown::aggregate::AggregationFunction> normal::plan::function::SumLogicalFunction::toExecutorReduceFunction() {
  if (name_.empty()) {
    name_ = fmt::format("sum-{}", this->expression()->alias());
  }
  return std::make_shared<normal::pushdown::aggregate::Sum>(name_, col(name_));
}


