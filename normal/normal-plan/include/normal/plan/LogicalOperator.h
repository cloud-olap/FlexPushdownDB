//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_LOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_LOGICALOPERATOR_H

#include <memory>
#include <string>

#include <normal/core/Operator.h>
#include "OperatorType.h"

namespace normal::plan {

class LogicalOperator {

public:
  explicit LogicalOperator(std::shared_ptr<OperatorType> Type);
  virtual ~LogicalOperator() = default;

  std::shared_ptr<OperatorType> type();

  virtual std::shared_ptr<core::Operator> toOperator() = 0;

  const std::string &getName() const;
  void setName(const std::string &Name);
  const std::shared_ptr<LogicalOperator> &getConsumer() const;
  void setConsumer(const std::shared_ptr<LogicalOperator> &Consumer);

private:
  std::shared_ptr<OperatorType> type_;
  std::string name_;
  std::shared_ptr<LogicalOperator> consumer_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_LOGICALOPERATOR_H
