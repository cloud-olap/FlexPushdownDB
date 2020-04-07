//
// Created by matt on 7/4/20.
//

#include "SumASTFunction.h"
#include "normal/core/expression/Expression.h"
#include "normal/pushdown/aggregate/Sum.h"

SumASTFunction::SumASTFunction() : AggregateFunction("sum") {}

std::shared_ptr<normal::pushdown::aggregate::AggregationFunction> SumASTFunction::toExecutorFunction() {

  // FIXME: Defaulting name to sum

  return std::make_shared<normal::pushdown::aggregate::Sum>("sum", this->expression());
}


