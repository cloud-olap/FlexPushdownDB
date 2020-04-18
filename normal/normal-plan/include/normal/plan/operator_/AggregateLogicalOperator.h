//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H

#include <vector>
#include <memory>

#include <normal/core/Operator.h>

#include <normal/plan/function/AggregateLogicalFunction.h>
#include <normal/plan/operator_/LogicalOperator.h>

namespace normal::plan::operator_ {

class AggregateLogicalOperator : public LogicalOperator {

public:
  explicit AggregateLogicalOperator(std::vector<std::shared_ptr<normal::plan::function::AggregateLogicalFunction>> Functions);

  [[nodiscard]] const std::vector<std::shared_ptr<normal::plan::function::AggregateLogicalFunction>> &functions() const;
  std::shared_ptr<std::vector<std::shared_ptr<core::Operator>>> toOperators() override;
  std::shared_ptr<core::Operator> toOperator() override;

private:
  std::vector<std::shared_ptr<normal::plan::function::AggregateLogicalFunction>> functions_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H
