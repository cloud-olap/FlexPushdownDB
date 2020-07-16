//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SCANLOGICALOPERATOR_H

#include <memory>
#include <normal/core/Operator.h>
#include <normal/plan/operator_/LogicalOperator.h>
#include <normal/connector/partition/PartitioningScheme.h>
#include <normal/expression/gandiva/Expression.h>

namespace normal::plan::operator_ {

class ScanLogicalOperator : public LogicalOperator {

public:
  explicit ScanLogicalOperator(std::shared_ptr<PartitioningScheme> partitioningScheme);
  ~ScanLogicalOperator() override = default;

  [[nodiscard]] const std::shared_ptr<PartitioningScheme> &getPartitioningScheme() const;

  void predicate(const std::shared_ptr<expression::gandiva::Expression> &predicate);

private:
  std::shared_ptr<PartitioningScheme> partitioningScheme_;

  std::shared_ptr<expression::gandiva::Expression> predicate_;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SCANLOGICALOPERATOR_H
