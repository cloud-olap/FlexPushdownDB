//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H

#include <vector>
#include <memory>

#include <normal/core/Operator.h>

#include <normal/sql/logical/AggregateLogicalFunction.h>
#include <normal/sql/logical/LogicalOperator.h>

namespace normal::sql::logical {

class AggregateLogicalOperator : public LogicalOperator {
private:
  std::vector<std::shared_ptr<AggregateLogicalFunction>> functions_;

public:
  explicit AggregateLogicalOperator(std::vector<std::shared_ptr<AggregateLogicalFunction>> functions);

  [[nodiscard]] const std::vector<std::shared_ptr<AggregateLogicalFunction>> &functions() const;

  std::shared_ptr<core::Operator> toOperator() override;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H
