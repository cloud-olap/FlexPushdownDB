//
// Created by matt on 7/4/20.
//

#include <normal/plan/operator_/S3SelectScanLogicalOperator.h>

#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/Or.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <variant>
#include <normal/connector/s3/S3Util.h>
#include <normal/pushdown/Util.h>

using namespace normal::plan::operator_;
using namespace normal::pushdown;
using namespace normal::core::type;

S3SelectScanLogicalOperator::S3SelectScanLogicalOperator(
	const std::shared_ptr<S3SelectPartitioningScheme>& partitioningScheme,
	std::shared_ptr<normal::pushdown::AWSClient> AwsClient) :
	ScanLogicalOperator(partitioningScheme),
	awsClient_(std::move(AwsClient)) {}


std::string S3SelectScanLogicalOperator::genSql(){
  // projected columns
  std::string colStr = "";
  for (auto colIndex = 0; colIndex < projectedColumnNames_->size(); colIndex++) {
    if (colIndex == 0) {
      colStr += projectedColumnNames_->at(colIndex);
    } else {
      colStr += ", " + projectedColumnNames_->at(colIndex);
    }
  }
  std::string sql = "select " + colStr + " from s3Object";

  // predicates
  if (predicate_ != nullptr) {
    std::string filterStr = predicate_->alias();
    sql += " where " + filterStr;
  }

  return sql;
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> S3SelectScanLogicalOperator::toOperators() {
  const int numRanges = 1;

  // preparation
  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  auto sql = genSql();
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  auto s3Bucket = std::static_pointer_cast<S3SelectPartition>(getPartitioningScheme()->partitions()->at(0))->getBucket();
  for (const auto &partition: *getPartitioningScheme()->partitions()) {
    auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);
    s3Objects->emplace_back(s3Partition->getObject());
  }
  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, *s3Objects, this->awsClient_->defaultS3Client());

  // construct physical operators
  for (const auto &partition: *getPartitioningScheme()->partitions()) {
    auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);
    auto s3Object = s3Partition->getObject();
    auto numBytes = partitionMap.find(s3Object)->second;
    auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, numRanges);

    for (const auto &scanRange: scanRanges) {
      auto scanOp = std::make_shared<normal::pushdown::S3SelectScan>(
              s3Partition->getBucket() + "/" + s3Object,
              s3Partition->getBucket(),
              s3Object,
              sql,
              *projectedColumnNames_,
              scanRange.first,
              scanRange.second,
              S3SelectCSVParseOptions(",", "\n"),
              this->awsClient_->defaultS3Client());
      operators->push_back(scanOp);
    }
  }

  return operators;
}

