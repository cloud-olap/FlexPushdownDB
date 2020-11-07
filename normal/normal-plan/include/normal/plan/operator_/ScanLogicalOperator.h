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

  void setPredicates(const std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> &predicates);

  void setProjectedColumnNames(const std::shared_ptr<std::vector<std::string>> &projectedColumnNames);

  const std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> &streamOutPhysicalOperators() const;

protected:
  std::pair<bool, std::shared_ptr<expression::gandiva::Expression>> checkPartitionValid(const std::shared_ptr<Partition>& partition);

  // projected columns, not final projection, but columns that downstream operators need
  // don't include columns that filters need, currently filters are integrated together with scan in logical plan
  std::shared_ptr<std::vector<std::string>> projectedColumnNames_;

  // ssb can push all filters to scan nodes, we can also make it more general: filterLogicalOperator
  // conjunctive predicates
  std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> predicates_;

  std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> streamOutPhysicalOperators_;

private:
  std::shared_ptr<PartitioningScheme> partitioningScheme_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SCANLOGICALOPERATOR_H
