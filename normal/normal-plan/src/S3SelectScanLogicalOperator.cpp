//
// Created by matt on 7/4/20.
//

#include "normal/plan/S3SelectScanLogicalOperator.h"

#include <normal/pushdown/S3SelectScan.h>

normal::plan::S3SelectScanLogicalOperator::S3SelectScanLogicalOperator(
	const std::shared_ptr<S3SelectPartitioningScheme>& partitioningScheme,
	std::shared_ptr<normal::pushdown::AWSClient> AwsClient) :
	ScanLogicalOperator(partitioningScheme),
	awsClient_(std::move(AwsClient)) {}

std::shared_ptr<normal::core::Operator> normal::plan::S3SelectScanLogicalOperator::toOperator() {

  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  for (const auto &partition: *getPartitioningScheme()->partitions()) {

	auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);

	// FIXME: Still unsure what to do with m_col? col could be an expression or an aggregate, or something else?

	auto scanOp = std::make_shared<normal::pushdown::S3SelectScan>(
		s3Partition->getBucket() + "/" + s3Partition->getObject(),
		s3Partition->getBucket(),
		s3Partition->getObject(),
		"select * from S3Object",
		s3Partition->getObject(),
		"A",
		this->awsClient_->defaultS3Client());

	operators->push_back(scanOp);
  }

  // FIXME: Should return multiple operators
  return operators->at(0);
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> normal::plan::S3SelectScanLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  for (const auto &partition: *getPartitioningScheme()->partitions()) {

	auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);

	// FIXME: Still unsure what to do with m_col? col could be an expression or an aggregate, or something else?

	auto scanOp = std::make_shared<normal::pushdown::S3SelectScan>(
		s3Partition->getBucket() + "/" + s3Partition->getObject(),
		s3Partition->getBucket(),
		s3Partition->getObject(),
		"select * from S3Object",
		s3Partition->getObject(),
		"A",
		this->awsClient_->defaultS3Client());

	operators->push_back(scanOp);
  }

  return operators;
}

