//
// Created by matt on 7/4/20.
//

#include "normal/plan/FileScanLogicalOperator.h"

#include <normal/pushdown/FileScan.h>

normal::plan::FileScanLogicalOperator::FileScanLogicalOperator(
	std::shared_ptr<LocalFilePartitioningScheme> partitioningScheme) :
	partitioningScheme_(std::move(partitioningScheme)) {}

std::shared_ptr<normal::core::Operator> normal::plan::FileScanLogicalOperator::toOperator() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  for(const auto& partition: partitioningScheme_->partitions()){
	operators->push_back(std::make_shared<normal::pushdown::FileScan>(this->name, partition->getPath()));
  }
  // FIXME: Should return multiple operators
  return operators->at(0);
}
