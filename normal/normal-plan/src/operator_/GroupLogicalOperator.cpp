//
// Created by Yifei Yang on 7/17/20.
//

#include <normal/plan/operator_/type/OperatorTypes.h>
#include "normal/plan/operator_/GroupLogicalOperator.h"

using namespace normal::plan::operator_;

normal::plan::operator_::GroupLogicalOperator::GroupLogicalOperator(const std::shared_ptr<std::vector<std::string>> &groupColumnNames,
                                                                    const std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> &functions,
                                                                    const std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> &projectExpression,
                                                                    const std::shared_ptr<LogicalOperator> &producer)
        : LogicalOperator(type::OperatorTypes::groupOperatorType()),
        groupColumnNames_(std::move(groupColumnNames)),
        functions_(std::move(functions)),
        projectExpression_(std::move(projectExpression)),
        producer_(std::move(producer)) {}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> GroupLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<core::Operator>>>();


  return std::shared_ptr<std::vector<std::shared_ptr<core::Operator>>>();
}

void GroupLogicalOperator::setNumConcurrentUnits(int numConcurrentUnits) {
  GroupLogicalOperator::numConcurrentUnits = numConcurrentUnits;
}

const std::shared_ptr<LogicalOperator> &GroupLogicalOperator::getProducer() const {
  return producer_;
}

