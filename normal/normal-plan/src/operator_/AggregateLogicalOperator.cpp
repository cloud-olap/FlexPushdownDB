//
// Created by matt on 2/4/20.
//

#include <normal/plan/operator_/AggregateLogicalOperator.h>

#include <normal/pushdown/Aggregate.h>
#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

using namespace normal::core::type;
using namespace normal::expression;

AggregateLogicalOperator::AggregateLogicalOperator(
	std::vector<std::shared_ptr<function::AggregateLogicalFunction>> functions)
    : LogicalOperator(type::OperatorTypes::aggregateOperatorType()),
    functions_(std::move(functions)) {}

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
