//
// Created by matt on 14/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_PROJECTLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_PROJECTLOGICALOPERATOR_H

#include <memory>
#include <vector>

#include <normal/core/Operator.h>
#include <normal/core/expression/Expression.h>

#include <normal/sql/logical/LogicalOperator.h>

namespace normal::sql::logical {

class ProjectLogicalOperator : public normal::sql::logical::LogicalOperator {

public:
  explicit ProjectLogicalOperator(std::vector<std::shared_ptr<core::expression::Expression>> expressions);

  [[nodiscard]] const std::vector<std::shared_ptr<core::expression::Expression>> &expressions() const;

  std::shared_ptr<core::Operator> toOperator() override;

private:
  std::vector<std::shared_ptr<core::expression::Expression>> expressions_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_PROJECTLOGICALOPERATOR_H
