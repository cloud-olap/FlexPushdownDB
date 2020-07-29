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
#include <normal/pushdown/merge/MergeOperator.h>

using namespace normal::plan::operator_;
using namespace normal::pushdown;
using namespace normal::core::type;

S3SelectScanLogicalOperator::S3SelectScanLogicalOperator(
	const std::shared_ptr<S3SelectPartitioningScheme>& partitioningScheme,
	std::shared_ptr<normal::pushdown::AWSClient> AwsClient) :
	ScanLogicalOperator(partitioningScheme),
	awsClient_(std::move(AwsClient)) {}


std::string S3SelectScanLogicalOperator::genFilterSql(){
  if (predicate_ != nullptr) {
    std::string filterStr = predicate_->alias();
    return " where " + filterStr;
  } else {
    return "";
  }
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> S3SelectScanLogicalOperator::toOperators() {
  const int numRanges = 1;

  // construct physical operators
  auto mode = getMode();
  switch (mode->id()) {
    case plan::operator_::mode::FullPushdown: return toOperatorsFullPushDown(numRanges);
    case plan::operator_::mode::PullupCaching: return toOperatorsPullupCaching(numRanges);
    default:
      throw std::domain_error("Unrecognized mode '" + mode->toString() + "'");
  }

}

std::shared_ptr<std::vector<std::shared_ptr<normal::pushdown::cache::CacheLoad>>>
S3SelectScanLogicalOperator::toCacheLoadOperators() {
  auto cacheLoadOperators = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::cache::CacheLoad>>>();

  // construct CacheLoad operators
  for (const auto &partition: *getPartitioningScheme()->partitions()) {

  }
}

std::shared_ptr<normal::pushdown::filter::Filter> S3SelectScanLogicalOperator::toFilterOperator() {
  return std::shared_ptr<normal::pushdown::filter::Filter>();
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>
S3SelectScanLogicalOperator::toOperatorsFullPushDown(int numRanges) {

  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  auto filterSql = genFilterSql();

  for (const auto &partition: *getPartitioningScheme()->partitions()) {
    auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);
    auto s3Object = s3Partition->getObject();
    auto numBytes = s3Partition->getNumBytes();
    auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, numRanges);

    int rangeId = 0;
    for (const auto &scanRange: scanRanges) {
      auto scanOp = S3SelectScan::make(
              "s3scan - " + s3Partition->getBucket() + "/" + s3Object + "-" + std::to_string(rangeId),
              s3Partition->getBucket(),
              s3Object,
              filterSql,
              *projectedColumnNames_,
              scanRange.first,
              scanRange.second,
              S3SelectCSVParseOptions(",", "\n"),
              this->awsClient_->defaultS3Client(),
              true);
      operators->emplace_back(scanOp);
      rangeId++;
    }
  }

  return operators;
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>
S3SelectScanLogicalOperator::toOperatorsPullupCaching(int numRanges) {

  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  auto scanColumnNames = projectedColumnNames_;
  if (predicate_) {
    auto filterColumnNames = predicate_->involvedColumnNames();
    scanColumnNames->insert(scanColumnNames->end(), filterColumnNames->begin(), filterColumnNames->end());
    // deduplicate
    auto scanColumnNameSet = std::make_shared<std::set<std::string>>(scanColumnNames->begin(), scanColumnNames->end());
    scanColumnNames->assign(scanColumnNameSet->begin(), scanColumnNameSet->end());
  }

  /**
   * For each range in each partition, construct:
   * a CacheLoad, a S3SelectScan, a MergeOperator, a Filter
   */
  streamOutPhysicalOperators_ = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();

  for (const auto &partition: *getPartitioningScheme()->partitions()) {
    auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);
    auto s3Object = s3Partition->getObject();
    auto numBytes = s3Partition->getNumBytes();
    auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, numRanges);

    int rangeId = 0;
    for (const auto &scanRange: scanRanges) {
      auto s3Bucket = s3Partition->getBucket();

      // S3SelectScan
      auto scanOp = S3SelectScan::make(
              "s3scan - " + s3Bucket + "/" + s3Object + "-" + std::to_string(rangeId),
              s3Bucket,
              s3Object,
              "",
              *scanColumnNames,     // actually useless, will use columnNames from ScanMessage
              scanRange.first,
              scanRange.second,
              S3SelectCSVParseOptions(",", "\n"),
              this->awsClient_->defaultS3Client(),
              false);
      operators->emplace_back(scanOp);

      // CacheLoad
      auto cacheLoad = pushdown::cache::CacheLoad::make(
              fmt::format("cacheLoad-{}/{}-{}", s3Bucket, s3Object, rangeId),
              *scanColumnNames,
              partition,
              scanRange.first,
              scanRange.second);
      operators->emplace_back(cacheLoad);

      // MergeOperator
      auto mergeOperator = merge::MergeOperator::make(
              fmt::format("merge-{}/{}-{}", s3Bucket, s3Object, rangeId));
      operators->emplace_back(mergeOperator);

      // wire up internally
      cacheLoad->setHitOperator(mergeOperator);
      mergeOperator->consume(cacheLoad);

      cacheLoad->setMissOperator(scanOp);
      scanOp->consume(cacheLoad);

      scanOp->produce(mergeOperator);
      mergeOperator->consume(scanOp);

      // Filter if it has filterPredicate
      if (predicate_) {
        auto filterPredicate = filter::FilterPredicate::make(predicate_);
        auto filter = filter::Filter::make(
                fmt::format("filter-{}/{}-{}", s3Bucket, s3Object, rangeId),
                filterPredicate);
        operators->emplace_back(filter);
        streamOutPhysicalOperators_->emplace_back(filter);

        mergeOperator->produce(filter);
        filter->consume(mergeOperator);
      } else {
        streamOutPhysicalOperators_->emplace_back(mergeOperator);
      }

      rangeId++;
    }
  }

  return operators;
}

