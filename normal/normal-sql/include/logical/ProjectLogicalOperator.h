//
// Created by matt on 14/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_LOGICAL_PROJECTLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_SRC_LOGICAL_PROJECTLOGICALOPERATOR_H

#include "LogicalOperator.h"

#include <normal/core/expression/Expressions.h>


class ProjectLogicalOperator : public LogicalOperator {

public:
  explicit ProjectLogicalOperator(std::vector<std::shared_ptr<normal::core::expression::Expression>> expressions);

  [[nodiscard]] const std::vector<std::shared_ptr<normal::core::expression::Expression>> &expressions() const;

  std::shared_ptr<normal::core::Operator> toOperator() override;

private:
  std::vector<std::shared_ptr<normal::core::expression::Expression>> expressions_;

};


#endif //NORMAL_NORMAL_SQL_SRC_LOGICAL_PROJECTLOGICALOPERATOR_H
