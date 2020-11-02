//
// Created by Yifei Yang on 7/17/20.
//

#include <normal/plan/operator_/type/OperatorTypes.h>
#include "normal/plan/operator_/GroupLogicalOperator.h"
#include <normal/pushdown/group/Group.h>

using namespace normal::plan::operator_;

normal::plan::operator_::GroupLogicalOperator::GroupLogicalOperator(const std::shared_ptr<std::vector<std::string>> &groupColumnNames,
                                                                    const std::shared_ptr<std::vector<std::string>> &aggregateColumnNames,
                                                                    const std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> &functions,
                                                                    const std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> &projectExpression,
                                                                    const std::shared_ptr<LogicalOperator> &producer)
        : LogicalOperator(type::OperatorTypes::groupOperatorType()),
        groupColumnNames_(std::move(groupColumnNames)),
        aggregateColumnNames_(std::move(aggregateColumnNames)),
        functions_(std::move(functions)),
        projectExpression_(std::move(projectExpression)),
        producer_(std::move(producer)) {}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> GroupLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<core::Operator>>>();

  for (auto index = 0; index < numConcurrentUnits_; index++) {
    auto expressions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    for (const auto &function: *functions_) {
      expressions->emplace_back(function->toExecutorFunction());
    }

    // FIXME: Defaulting to name -> group
    auto group = std::make_shared<normal::pushdown::group::Group>(fmt::format("group-{}", index),
                                                                  *groupColumnNames_,
                                                                  *aggregateColumnNames_,
                                                                  expressions,
                                                                  getQueryId());
    operators->emplace_back(group);
  }

  // add group reduce if needed
  if (numConcurrentUnits_ > 1) {
    auto reduceExpressions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
    std::vector<std::string> aggregateColumnNames;
    for (const auto &function: *functions_) {
      reduceExpressions->emplace_back(function->toExecutorReduceFunction());
      aggregateColumnNames.emplace_back(function->getName());
    }

    auto groupReduce = std::make_shared<normal::pushdown::group::Group>("groupReduce",
                                                                        *groupColumnNames_,
                                                                        aggregateColumnNames,
                                                                        reduceExpressions,
                                                                        getQueryId());

    // wire up internally
    for (const auto &group: *operators) {
      group->produce(groupReduce);
      groupReduce->consume(group);
    }
    operators->emplace_back(groupReduce);
  }

  return operators;
}

void GroupLogicalOperator::setNumConcurrentUnits(int numConcurrentUnits) {
  numConcurrentUnits_ = numConcurrentUnits;
}

const std::shared_ptr<LogicalOperator> &GroupLogicalOperator::getProducer() const {
  return producer_;
}

