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

void ScanLogicalOperator::predicate(const std::shared_ptr<expression::gandiva::Expression> &predicate) {
  predicate_ = predicate;
}

void ScanLogicalOperator::setPredicate(const std::shared_ptr<expression::gandiva::Expression> &predicate) {
  predicate_ = predicate;
}

void
ScanLogicalOperator::setProjectedColumnNames(const std::shared_ptr<std::vector<std::string>> &projectedColumnNames) {
  projectedColumnNames_ = projectedColumnNames;
}

const std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> &
ScanLogicalOperator::streamOutPhysicalOperators() const {
  return streamOutPhysicalOperators_;
}
