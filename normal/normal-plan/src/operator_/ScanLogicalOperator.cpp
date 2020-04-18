//
// Created by matt on 1/4/20.
//

#include <normal/plan/operator_/ScanLogicalOperator.h>
#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

ScanLogicalOperator::ScanLogicalOperator(
	std::shared_ptr<PartitioningScheme> partitioningScheme) :
	LogicalOperator(type::OperatorTypes::scanOperatorType()),
	partitioningScheme_(std::move(partitioningScheme)) {}

const std::shared_ptr<PartitioningScheme> &ScanLogicalOperator::getPartitioningScheme() const {
  return partitioningScheme_;
}
