//
// Created by matt on 14/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_PROJECTLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_PROJECTLOGICALOPERATOR_H

#include <memory>
#include <vector>

#include <normal/core/Operator.h>
#include <normal/expression/Expression.h>

#include <normal/plan/operator_/LogicalOperator.h>

namespace normal::plan::operator_ {

class ProjectLogicalOperator : public LogicalOperator {

public:
  explicit ProjectLogicalOperator(std::vector<std::shared_ptr<normal::expression::Expression>> expressions);

  [[nodiscard]] const std::vector<std::shared_ptr<expression::Expression>> &expressions() const;

  std::shared_ptr<core::Operator> toOperator() override;
  std::shared_ptr<std::vector<std::shared_ptr<core::Operator>>> toOperators() override;

private:
  // FIXME: This should probably be a pointer
  std::vector<std::shared_ptr<expression::Expression>> expressions_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_PROJECTLOGICALOPERATOR_H
