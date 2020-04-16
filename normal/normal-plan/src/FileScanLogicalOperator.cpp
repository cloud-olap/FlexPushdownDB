//
// Created by matt on 7/4/20.
//

#include "normal/plan/FileScanLogicalOperator.h"

#include <normal/pushdown/FileScan.h>
#include <normal/plan/OperatorTypes.h>

normal::plan::FileScanLogicalOperator::FileScanLogicalOperator(
	const std::shared_ptr<LocalFilePartitioningScheme>& partitioningScheme) :
	ScanLogicalOperator(partitioningScheme) {}

std::shared_ptr<normal::core::Operator> normal::plan::FileScanLogicalOperator::toOperator() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  for (const auto &partition: *getPartitioningScheme()->partitions()) {
    auto localFilePartition = std::static_pointer_cast<LocalFilePartition>(partition);
	operators->push_back(std::make_shared<normal::pushdown::FileScan>(localFilePartition->getPath(), localFilePartition->getPath()));
  }
  // FIXME: Should return multiple operators
  return operators->at(0);
}
