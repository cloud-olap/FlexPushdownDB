//
// Created by matt on 7/4/20.
//

#include "normal/plan/SumLogicalFunction.h"

#include <normal/pushdown/aggregate/Sum.h>

normal::plan::SumLogicalFunction::SumLogicalFunction() : AggregateLogicalFunction("sum") {}

std::shared_ptr<normal::pushdown::aggregate::AggregationFunction> normal::plan::SumLogicalFunction::toExecutorFunction() {

  // FIXME: Defaulting name to sum

  return std::make_shared<normal::pushdown::aggregate::Sum>("sum", this->expression());
}


