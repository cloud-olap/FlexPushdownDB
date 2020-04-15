//
// Created by matt on 7/4/20.
//

#include "normal/sql/logical/SumLogicalFunction.h"

#include <memory>

#include "normal/pushdown/aggregate/AggregationFunction.h"
#include "normal/pushdown/aggregate/Sum.h"

SumLogicalFunction::SumLogicalFunction() : AggregateLogicalFunction("sum") {}

std::shared_ptr<normal::pushdown::aggregate::AggregationFunction> SumLogicalFunction::toExecutorFunction() {

  // FIXME: Defaulting name to sum

  return std::make_shared<normal::pushdown::aggregate::Sum>("sum", this->expression());
}


