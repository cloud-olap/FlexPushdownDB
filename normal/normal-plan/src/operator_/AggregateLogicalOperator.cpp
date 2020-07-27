//
// Created by matt on 2/4/20.
//

#include <normal/plan/operator_/AggregateLogicalOperator.h>

#include <normal/pushdown/Aggregate.h>
#include <normal/plan/operator_/type/OperatorTypes.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/expression/gandiva/Divide.h>
#include <normal/expression/gandiva/Add.h>
#include <normal/expression/gandiva/Subtract.h>

using namespace normal::plan::operator_;

using namespace normal::core::type;
using namespace normal::expression;

AggregateLogicalOperator::AggregateLogicalOperator(
	std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> functions,
	std::shared_ptr<LogicalOperator> producer)
    : LogicalOperator(type::OperatorTypes::aggregateOperatorType()),
    functions_(std::move(functions)),
    producer_(std::move(producer)) {}


std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> AggregateLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<core::Operator>>>();

  for (auto index = 0; index < numConcurrentUnits_; index++) {
    auto expressions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    for (const auto &function: *functions_) {
      expressions->emplace_back(function->toExecutorFunction());
    }

    // FIXME: Defaulting to name -> aggregation
    auto aggregate = std::make_shared<normal::pushdown::Aggregate>(fmt::format("aggregate-{}", index), expressions);
    operators->emplace_back(aggregate);
  }

  // add aggregate reduce if needed
  if (numConcurrentUnits_ > 1) {
    auto reduceExpressions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    for (const auto &function: *functions_) {
      reduceExpressions->emplace_back(function->toExecutorReduceFunction());
    }
    auto aggregateReduce = std::make_shared<normal::pushdown::Aggregate>("aggregateReduce", reduceExpressions);

    // wire up internally
    for (const auto &aggregate: *operators) {
      aggregate->produce(aggregateReduce);
      aggregateReduce->consume(aggregate);
    }
    operators->emplace_back(aggregateReduce);
  }

  return operators;
}

const std::shared_ptr<LogicalOperator> &AggregateLogicalOperator::getProducer() const {
  return producer_;
}

void AggregateLogicalOperator::setNumConcurrentUnits(int numConcurrentUnits) {
  numConcurrentUnits_ = numConcurrentUnits;
}

std::shared_ptr<normal::expression::gandiva::Expression> normal::plan::operator_::castToFloat64Type(
        std::shared_ptr<normal::expression::gandiva::Expression> expr) {
  if (typeid(*expr) == typeid(normal::expression::gandiva::Column)) {
    return cast(expr, normal::core::type::float64Type());
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::Multiply)) {
    auto biExpr = std::static_pointer_cast<normal::expression::gandiva::Multiply>(expr);
    auto leftExpr = biExpr->getLeft();
    auto rightExpr = biExpr->getRight();
    return times(castToFloat64Type(leftExpr), castToFloat64Type(rightExpr));
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::Divide)) {
    auto biExpr = std::static_pointer_cast<normal::expression::gandiva::Divide>(expr);
    auto leftExpr = biExpr->getLeft();
    auto rightExpr = biExpr->getRight();
    return divide(castToFloat64Type(leftExpr), castToFloat64Type(rightExpr));
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::Add)) {
    auto biExpr = std::static_pointer_cast<normal::expression::gandiva::Add>(expr);
    auto leftExpr = biExpr->getLeft();
    auto rightExpr = biExpr->getRight();
    return plus(castToFloat64Type(leftExpr), castToFloat64Type(rightExpr));
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::Subtract)) {
    auto biExpr = std::static_pointer_cast<normal::expression::gandiva::Subtract>(expr);
    auto leftExpr = biExpr->getLeft();
    auto rightExpr = biExpr->getRight();
    return minus(castToFloat64Type(leftExpr), castToFloat64Type(rightExpr));
  } else {
    std::runtime_error("Unsupported expressions in aggregation functions");
    return nullptr;
  }
}