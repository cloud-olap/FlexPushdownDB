//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_LOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_LOGICALOPERATOR_H

#include <memory>
#include <string>

#include <normal/core/Operator.h>

namespace normal::plan {

class LogicalOperator {
public:
  virtual ~LogicalOperator() = default;
  std::string name;
  std::shared_ptr<LogicalOperator> consumer;
  virtual std::shared_ptr<core::Operator> toOperator() = 0;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_LOGICALOPERATOR_H
