//
// Created by matt on 2/4/20.
//

#include <normal/plan/operator_/AggregateLogicalOperator.h>

#include <normal/pushdown/Aggregate.h>
#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

using namespace normal::core::type;
using namespace normal::core::expression;

AggregateLogicalOperator::AggregateLogicalOperator(
	std::vector<std::shared_ptr<normal::plan::function::AggregateLogicalFunction>> Functions)
    : LogicalOperator(operator_::type::OperatorTypes::aggregateOperatorType()),
    functions_(std::move(Functions)) {}

const std::vector<std::shared_ptr<normal::plan::function::AggregateLogicalFunction>> &AggregateLogicalOperator::functions() const {
  return functions_;
}

std::shared_ptr<normal::core::Operator> AggregateLogicalOperator::toOperator() {

  auto expressions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();

  for (const auto &function: functions_) {
    expressions->push_back(function->toExecutorFunction());
  }

  // FIXME: Defaulting to name -> agg
  auto aggregateExecutor = std::make_shared<normal::pushdown::Aggregate>("agg", expressions);

  return aggregateExecutor;
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> AggregateLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<core::Operator>>>();
  operators->push_back(this->toOperator());
  return operators;
}
