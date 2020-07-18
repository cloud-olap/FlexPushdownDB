//
// Created by Yifei Yang on 7/17/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_GROUPLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_GROUPLOGICALOPERATOR_H

#include <normal/core/Operator.h>
#include <normal/plan/operator_/LogicalOperator.h>
#include <normal/plan/function/AggregateLogicalFunction.h>

namespace normal::plan::operator_ {

class GroupLogicalOperator : public LogicalOperator {

public:
  GroupLogicalOperator(const std::shared_ptr<std::vector<std::string>> &groupColumnNames,
                       const std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> &functions,
                       const std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> &projectExpression);

  std::shared_ptr<std::vector<std::shared_ptr<core::Operator>>> toOperators() override;
  std::shared_ptr<core::Operator> toOperator() override;

private:
  std::shared_ptr<std::vector<std::string>> groupColumnNames_;
  std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> functions_;
  std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> projectExpression_;
};

}


#endif //NORMAL_GROUPLOGICALOPERATOR_H
