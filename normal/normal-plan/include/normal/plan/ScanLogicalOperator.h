//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SCANLOGICALOPERATOR_H

#include <memory>

#include <normal/core/Operator.h>

#include <normal/plan/LogicalOperator.h>
#include <normal/connector/partition/PartitioningScheme.h>

namespace normal::plan {

class ScanLogicalOperator : public LogicalOperator {

public:
  explicit ScanLogicalOperator(std::shared_ptr<PartitioningScheme> partitioningScheme);
  ~ScanLogicalOperator() override = default;

  [[nodiscard]] const std::shared_ptr<PartitioningScheme> &getPartitioningScheme() const;

  std::shared_ptr<core::Operator> toOperator() override = 0;

private:
  std::shared_ptr<PartitioningScheme> partitioningScheme_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SCANLOGICALOPERATOR_H
