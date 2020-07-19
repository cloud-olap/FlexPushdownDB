//
// Created by matt on 7/4/20.
//

#include <normal/plan/operator_/S3SelectScanLogicalOperator.h>

#include <normal/pushdown/s3/S3SelectScan.h>

using namespace normal::plan::operator_;
using namespace normal::pushdown;

S3SelectScanLogicalOperator::S3SelectScanLogicalOperator(
	const std::shared_ptr<S3SelectPartitioningScheme>& partitioningScheme,
	std::shared_ptr<normal::pushdown::AWSClient> AwsClient) :
	ScanLogicalOperator(partitioningScheme),
	awsClient_(std::move(AwsClient)) {}

std::shared_ptr<normal::core::Operator> S3SelectScanLogicalOperator::toOperator() {

  // FIXME: This doesn't make sense anymore

  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  for (const auto &partition: *getPartitioningScheme()->partitions()) {

	auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);

	// FIXME: Still unsure what to do with m_col? col could be an expression or an aggregate, or something else?

	// FIXME: Feels like there is a conceptual difference between a s3 select scan that contains
	//  push down operators vs one that simply brings back raw data?

	std::vector<std::string> columns = {"A"};

	auto scanOp = std::make_shared<normal::pushdown::S3SelectScan>(
		s3Partition->getBucket() + "/" + s3Partition->getObject(),
		s3Partition->getBucket(),
		s3Partition->getObject(),
		"select * from S3Object",
		columns,
		0,
		1023,
		S3SelectCSVParseOptions(",", "\n"),
		this->awsClient_->defaultS3Client());

	operators->push_back(scanOp);
  }

  // FIXME: Should return multiple operators
  return operators->at(0);
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> S3SelectScanLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  for (const auto &partition: *getPartitioningScheme()->partitions()) {

    auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);

    // FIXME: Still unsure what to do with m_col? col could be an expression or an aggregate, or something else?

    // FIXME: Feels like there is a conceptual difference between a s3 select scan that contains
    //  push down operators vs one that simply brings back raw data?

    std::vector<std::string> columns = {"A"};

    auto scanOp = std::make_shared<normal::pushdown::S3SelectScan>(
      s3Partition->getBucket() + "/" + s3Partition->getObject(),
      s3Partition->getBucket(),
      s3Partition->getObject(),
      "select * from S3Object",
      columns,
      0,
      1023,
      S3SelectCSVParseOptions(",", "\n"),
      this->awsClient_->defaultS3Client());

    operators->push_back(scanOp);
  }

  return operators;
}

