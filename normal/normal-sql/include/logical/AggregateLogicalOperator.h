//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_AGGREGATELOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_SRC_AST_AGGREGATELOGICALOPERATOR_H

#include <vector>

#include "logical/AggregateLogicalFunction.h"
#include "logical/LogicalOperator.h"

class AggregateLogicalOperator : public LogicalOperator {
private:
  std::vector<std::shared_ptr<AggregateLogicalFunction>> functions_;

public:
  explicit AggregateLogicalOperator(std::vector<std::shared_ptr<AggregateLogicalFunction>> functions);

  [[nodiscard]] const std::vector<std::shared_ptr<AggregateLogicalFunction>> &functions() const;

  std::shared_ptr<normal::core::Operator> toOperator() override;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_AGGREGATELOGICALOPERATOR_H
