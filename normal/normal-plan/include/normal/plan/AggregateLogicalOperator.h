//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H

#include <vector>
#include <memory>

#include <normal/core/Operator.h>

#include "AggregateLogicalFunction.h"
#include "LogicalOperator.h"

namespace normal::plan {

class AggregateLogicalOperator : public LogicalOperator {
private:
  std::vector<std::shared_ptr<AggregateLogicalFunction>> functions_;

public:
  explicit AggregateLogicalOperator(std::vector<std::shared_ptr<normal::plan::AggregateLogicalFunction>> Functions);

  [[nodiscard]] const std::vector<std::shared_ptr<AggregateLogicalFunction>> &functions() const;

  std::shared_ptr<core::Operator> toOperator() override;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H
