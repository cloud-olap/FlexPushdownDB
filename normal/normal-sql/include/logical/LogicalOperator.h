//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_LOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_SRC_AST_LOGICALOPERATOR_H

#include <normal/core/Operator.h>

class LogicalOperator {
public:
  virtual ~LogicalOperator() = default;
  std::string name;
  std::shared_ptr<LogicalOperator> consumer;
  virtual std::shared_ptr<normal::core::Operator> toOperator() = 0;

};

#endif //NORMAL_NORMAL_SQL_SRC_AST_LOGICALOPERATOR_H
