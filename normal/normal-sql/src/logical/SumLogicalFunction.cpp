//
// Created by matt on 7/4/20.
//

#include <normal/sql/logical/SumLogicalFunction.h>

#include <normal/pushdown/aggregate/Sum.h>

normal::sql::logical::SumLogicalFunction::SumLogicalFunction() : AggregateLogicalFunction("sum") {}

std::shared_ptr<normal::pushdown::aggregate::AggregationFunction> normal::sql::logical::SumLogicalFunction::toExecutorFunction() {

  // FIXME: Defaulting name to sum

  return std::make_shared<normal::pushdown::aggregate::Sum>("sum", this->expression());
}


