//
// Created by matt on 2/4/20.
//

#include "AggregateNode.h"
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/core/expression/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/core/expression/Cast.h>

#include <utility>

using namespace normal::core::type;
using namespace normal::core::expression;

AggregateNode::AggregateNode(std::vector<std::shared_ptr<AggregateFunction>> Functions)
    : functions_(std::move(Functions)) {}

const std::vector<std::shared_ptr<AggregateFunction>> &AggregateNode::functions() const {
  return functions_;
}

std::shared_ptr<normal::core::Operator> AggregateNode::toOperator() {

  auto expressions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();

  for (const auto &function: functions_) {
    expressions->push_back(function->toExecutorFunction());
  }

  // FIXME: Defaulting to name -> agg
  auto aggregateExecutor = std::make_shared<normal::pushdown::Aggregate>("agg", expressions);

  return aggregateExecutor;
}
