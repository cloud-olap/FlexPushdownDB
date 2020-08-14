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
#include <normal/pushdown/merge/Merge.h>
#include <normal/pushdown/Project.h>
#include <normal/pushdown/Globals.h>

using namespace normal::plan::operator_;
using namespace normal::pushdown;
using namespace normal::core::type;

S3SelectScanLogicalOperator::S3SelectScanLogicalOperator(
	const std::shared_ptr<S3SelectPartitioningScheme>& partitioningScheme) :
	ScanLogicalOperator(partitioningScheme) {}


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
    case plan::operator_::mode::HybridCaching: return toOperatorsHybridCaching(numRanges);
    default:
      SPDLOG_ERROR("Unrecognized mode: '{}'", mode->toString());
  }

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
              "s3select - " + s3Partition->getBucket() + "/" + s3Object + "-" + std::to_string(rangeId),
              s3Partition->getBucket(),
              s3Object,
              filterSql,
              *projectedColumnNames_,
              scanRange.first,
              scanRange.second,
              S3SelectCSVParseOptions(",", "\n"),
              pushdown::defaultS3Client,
              true,
              false);
      operators->emplace_back(scanOp);
      rangeId++;
    }
  }

  return operators;
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>
S3SelectScanLogicalOperator::toOperatorsPullupCaching(int numRanges) {

  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  auto predicateColumnNames = std::make_shared<std::vector<std::string>>();
  std::shared_ptr<pushdown::filter::FilterPredicate> filterPredicate;

  if (predicate_) {
    // predicate column names
    predicateColumnNames = predicate_->involvedColumnNames();

    // simpleCast
    filterPredicate = filter::FilterPredicate::make(predicate_);
    filterPredicate->simpleCast(filterPredicate->expression());
  }

  /**
   * For each range in each partition, construct:
   * a CacheLoad, a S3SelectScan, a Merge, a Filter if needed
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
              *projectedColumnNames_,     // actually useless, will use columnNames from ScanMessage
              scanRange.first,
              scanRange.second,
              S3SelectCSVParseOptions(",", "\n"),
              pushdown::defaultS3Client,
              false,
              true);
      operators->emplace_back(scanOp);

      // CacheLoad
      auto cacheLoad = pushdown::cache::CacheLoad::make(
              fmt::format("cacheLoad-{}/{}-{}", s3Bucket, s3Object, rangeId),
              *projectedColumnNames_,
              *predicateColumnNames,
              partition,
              scanRange.first,
              scanRange.second);
      operators->emplace_back(cacheLoad);

      // Merge
      auto merge = merge::Merge::make(
              fmt::format("merge-{}/{}-{}", s3Bucket, s3Object, rangeId));
      operators->emplace_back(merge);

      // wire up internally
      cacheLoad->setHitOperator(merge);
      merge->setLeftProducer(cacheLoad);

      cacheLoad->setMissOperatorToCache(scanOp);
      scanOp->consume(cacheLoad);

      scanOp->produce(merge);
      merge->setRightProducer(scanOp);

      // Filter if it has filterPredicate
      if (predicate_) {
        auto filter = filter::Filter::make(
                fmt::format("filter-{}/{}-{}", s3Bucket, s3Object, rangeId),
                filterPredicate);
        operators->emplace_back(filter);
        streamOutPhysicalOperators_->emplace_back(filter);

        merge->produce(filter);
        filter->consume(merge);
      } else {
        streamOutPhysicalOperators_->emplace_back(merge);
      }

      rangeId++;
    }
  }

  return operators;
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>
S3SelectScanLogicalOperator::toOperatorsHybridCaching(int numRanges) {

  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  auto predicateColumnNames = std::make_shared<std::vector<std::string>>();
  std::shared_ptr<pushdown::filter::FilterPredicate> filterPredicate;

  if (predicate_) {
    // predicate column names
    predicateColumnNames = predicate_->involvedColumnNames();
    // deduplicate
    auto predicateColumnNameSet = std::make_shared<std::set<std::string>>(predicateColumnNames->begin(), predicateColumnNames->end());
    predicateColumnNames->assign(predicateColumnNameSet->begin(), predicateColumnNameSet->end());

    // simpleCast
    filterPredicate = filter::FilterPredicate::make(predicate_);
    filterPredicate->simpleCast(filterPredicate->expression());
  }

  /**
   * For each range in each partition, construct:
   * a CacheLoad, a S3Scan which is to pull up segments to cache, a Merge, a Filter if needed
   * a S3Select, a second Merge for local filtered segments + S3Select result
   * a Project to make all tupleSets have the same schema
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

      // cacheLoad
      auto cacheLoad = pushdown::cache::CacheLoad::make(
              fmt::format("cacheLoad-{}/{}-{}", s3Bucket, s3Object, rangeId),
              *projectedColumnNames_,
              *predicateColumnNames,
              partition,
              scanRange.first,
              scanRange.second);
      operators->emplace_back(cacheLoad);

      // s3Scan
      auto s3Scan = S3SelectScan::make(
              "s3scan (to cache) - " + s3Bucket + "/" + s3Object + "-" + std::to_string(rangeId),
              s3Bucket,
              s3Object,
              "",
              *projectedColumnNames_,     // actually useless, will use columnNames from ScanMessage
              scanRange.first,
              scanRange.second,
              S3SelectCSVParseOptions(",", "\n"),
              pushdown::defaultS3Client,
              false,
              true);
      operators->emplace_back(s3Scan);

      // merge1
      auto merge1 = merge::Merge::make(
              fmt::format("merge1-{}/{}-{}", s3Bucket, s3Object, rangeId));
      operators->emplace_back(merge1);

      // wire up cacheLoad, s3Scan, merge1
      cacheLoad->setHitOperator(merge1);
      merge1->setLeftProducer(cacheLoad);

      cacheLoad->setMissOperatorToCache(s3Scan);
      s3Scan->consume(cacheLoad);

      s3Scan->produce(merge1);
      merge1->setRightProducer(s3Scan);

      // filter if it has filterPredicate
      std::shared_ptr<normal::core::Operator> leftOpOfMerge2;
      if (predicate_) {
        auto filter = filter::Filter::make(
                fmt::format("filter-{}/{}-{}", s3Bucket, s3Object, rangeId),
                filterPredicate);
        operators->emplace_back(filter);
        leftOpOfMerge2 = filter;

        merge1->produce(filter);
        filter->consume(merge1);
      } else {
        leftOpOfMerge2 = merge1;
      }

      // s3Select
      auto s3Select = S3SelectScan::make(
              "s3select - " + s3Bucket + "/" + s3Object + "-" + std::to_string(rangeId),
              s3Bucket,
              s3Object,
              genFilterSql(),
              *projectedColumnNames_,     // actually useless, will use columnNames from ScanMessage
              scanRange.first,
              scanRange.second,
              S3SelectCSVParseOptions(",", "\n"),
              pushdown::defaultS3Client,
              false,
              false);
      operators->emplace_back(s3Select);

      // merge2
      auto merge2 = merge::Merge::make(
              fmt::format("merge2-{}/{}-{}", s3Bucket, s3Object, rangeId));
      operators->emplace_back(merge2);

      // wire up leftOpOfMerge2, s3Select, merge2 with the part before
      cacheLoad->setMissOperatorToPushdown(s3Select);
      s3Select->consume(cacheLoad);

      leftOpOfMerge2->produce(merge2);
      merge2->setLeftProducer(leftOpOfMerge2);

      s3Select->produce(merge2);
      merge2->setRightProducer(s3Select);

      // project
      auto projectExpressions = std::make_shared<std::vector<std::shared_ptr<expression::gandiva::Expression>>>();
      for (auto const &projectedColumnName: *projectedColumnNames_) {
        projectExpressions->emplace_back(expression::gandiva::col(projectedColumnName));
      }
      auto project = std::make_shared<Project>(
              fmt::format("project-{}/{}-{}", s3Bucket, s3Object, rangeId), *projectExpressions);
      operators->emplace_back(project);

      // wire up merge2 and project
      merge2->produce(project);
      project->consume(merge2);

      // project is the stream-out physical operator
      streamOutPhysicalOperators_->emplace_back(project);

      rangeId++;
    }
  }

  return operators;
}

