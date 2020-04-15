//
// Created by matt on 7/4/20.
//

#include <normal/sql/logical/S3SelectScanLogicalOperator.h>

#include <normal/pushdown/S3SelectScan.h>

normal::sql::logical::S3SelectScanLogicalOperator::S3SelectScanLogicalOperator(std::shared_ptr<
	S3SelectPartitioningScheme> partitioningScheme,
																			   std::shared_ptr<normal::pushdown::AWSClient> AwsClient)
	: partitioningScheme_(std::move(partitioningScheme)),
	  awsClient_(std::move(AwsClient)) {}

std::shared_ptr<normal::core::Operator> normal::sql::logical::S3SelectScanLogicalOperator::toOperator() {

  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  for (const auto &partition: partitioningScheme_->partitions()) {

    // FIXME: Still unsure what to do with m_col? col could be an expression or an aggregate, or something else?

	auto scanOp = std::make_shared<normal::pushdown::S3SelectScan>(
		partition->getBucket() + "/" + partition->getObject(),
		partition->getBucket(),
		partition->getObject(),
		"select * from S3Object",
		partition->getObject(),
		"A",
		this->awsClient_->defaultS3Client());

	operators->push_back(scanOp);
  }

  // FIXME: Should return multiple operators
  return operators->at(0);
}

