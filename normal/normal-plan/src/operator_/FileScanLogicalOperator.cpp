//
// Created by matt on 7/4/20.
//

#include <normal/plan/operator_/FileScanLogicalOperator.h>

#include <normal/pushdown/FileScan.h>
#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

FileScanLogicalOperator::FileScanLogicalOperator(
	const std::shared_ptr<LocalFilePartitioningScheme> &partitioningScheme) :
	ScanLogicalOperator(partitioningScheme) {}

std::shared_ptr<normal::core::Operator> FileScanLogicalOperator::toOperator() {

  // FIXME: This doesn't make sense anymore, need to delete somehow

  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();

  for (const auto &partition: *getPartitioningScheme()->partitions()) {
	auto localFilePartition = std::static_pointer_cast<LocalFilePartition>(partition);

	const std::shared_ptr<pushdown::FileScan> &fileScanOperator =
		std::make_shared<normal::pushdown::FileScan>(localFilePartition->getPath(),
													 localFilePartition->getPath());
	operators->push_back(fileScanOperator);
  }

  return operators->at(0);
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> FileScanLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  for (const auto &partition: *getPartitioningScheme()->partitions()) {
	auto localFilePartition = std::static_pointer_cast<LocalFilePartition>(partition);

	const std::shared_ptr<pushdown::FileScan> &fileScanOperator =
		std::make_shared<normal::pushdown::FileScan>(localFilePartition->getPath(),
													 localFilePartition->getPath());


	operators->push_back(fileScanOperator);
  }
  return operators;
}
