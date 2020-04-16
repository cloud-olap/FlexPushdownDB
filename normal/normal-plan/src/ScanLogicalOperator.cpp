//
// Created by matt on 1/4/20.
//

#include <normal/plan/ScanLogicalOperator.h>

#include <normal/plan/OperatorTypes.h>

#include <utility>

normal::plan::ScanLogicalOperator::ScanLogicalOperator(
	std::shared_ptr<PartitioningScheme> partitioningScheme) :
	LogicalOperator(OperatorTypes::scanOperatorType()),
	partitioningScheme_(std::move(partitioningScheme)) {}

const std::shared_ptr<PartitioningScheme> &normal::plan::ScanLogicalOperator::getPartitioningScheme() const {
  return partitioningScheme_;
}
