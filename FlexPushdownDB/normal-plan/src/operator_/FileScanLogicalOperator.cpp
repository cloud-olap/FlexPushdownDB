//
// Created by matt on 7/4/20.
//

#include <normal/plan/operator_/FileScanLogicalOperator.h>

#include <normal/pushdown/file/FileScan.h>
#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;
using namespace normal::pushdown::file;

FileScanLogicalOperator::FileScanLogicalOperator(
	const std::shared_ptr<LocalFilePartitioningScheme> &partitioningScheme) :
	ScanLogicalOperator(partitioningScheme) {}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> FileScanLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  for (const auto &partition: *getPartitioningScheme()->partitions()) {
	auto localFilePartition = std::static_pointer_cast<LocalFilePartition>(partition);

	const std::shared_ptr<FileScan> &fileScanOperator =
		std::make_shared<FileScan>(localFilePartition->getPath(),
													 localFilePartition->getPath(),
													 getQueryId());


	operators->push_back(fileScanOperator);
  }
  return operators;
}
