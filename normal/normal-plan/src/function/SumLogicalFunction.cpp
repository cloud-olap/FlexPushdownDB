//
// Created by matt on 7/4/20.
//

#include <normal/plan/function/SumLogicalFunction.h>

#include <normal/pushdown/aggregate/Sum.h>

using namespace normal::plan::function;

normal::plan::function::SumLogicalFunction::SumLogicalFunction() : normal::plan::function::AggregateLogicalFunction("sum") {}

std::shared_ptr<normal::pushdown::aggregate::AggregationFunction> normal::plan::function::SumLogicalFunction::toExecutorFunction() {

  // FIXME: Defaulting name to sum

  return std::make_shared<normal::pushdown::aggregate::Sum>("sum", this->expression());
}


