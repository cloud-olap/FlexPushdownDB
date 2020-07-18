//
// Created by Yifei Yang on 7/17/20.
//

#include <normal/plan/operator_/type/OperatorTypes.h>
#include "normal/plan/operator_/GroupLogicalOperator.h"

using namespace normal::plan::operator_;

normal::plan::operator_::GroupLogicalOperator::GroupLogicalOperator(const std::shared_ptr<std::vector<std::string>> &groupColumnNames,
                                                                    const std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> &functions,
                                                                    const std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> &projectExpression)
        : LogicalOperator(type::OperatorTypes::groupOperatorType()),
        groupColumnNames_(groupColumnNames),
        functions_(functions),
        projectExpression_(projectExpression) {}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> GroupLogicalOperator::toOperators() {
  return std::shared_ptr<std::vector<std::shared_ptr<core::Operator>>>();
}

std::shared_ptr<normal::core::Operator> GroupLogicalOperator::toOperator() {
  return std::shared_ptr<core::Operator>();
}
