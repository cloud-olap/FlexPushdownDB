//
// Created by matt on 2/4/20.
//

#include <normal/plan/AggregateLogicalOperator.h>

#include <normal/pushdown/Aggregate.h>

using namespace normal::core::type;
using namespace normal::core::expression;

normal::plan::AggregateLogicalOperator::AggregateLogicalOperator(std::vector<std::shared_ptr<normal::plan::AggregateLogicalFunction>> Functions)
    : functions_(std::move(Functions)) {}

const std::vector<std::shared_ptr<normal::plan::AggregateLogicalFunction>> &normal::plan::AggregateLogicalOperator::functions() const {
  return functions_;
}

std::shared_ptr<normal::core::Operator> normal::plan::AggregateLogicalOperator::toOperator() {

  auto expressions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();

  for (const auto &function: functions_) {
    expressions->push_back(function->toExecutorFunction());
  }

  // FIXME: Defaulting to name -> agg
  auto aggregateExecutor = std::make_shared<normal::pushdown::Aggregate>("agg", expressions);

  return aggregateExecutor;
}
