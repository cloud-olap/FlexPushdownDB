//
// Created by matt on 7/4/20.
//

#include <normal/plan/operator_/S3SelectScanLogicalOperator.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/pushdown/Util.h>
#include <normal/pushdown/merge/Merge.h>
#include <normal/pushdown/Project.h>
#include <normal/plan/Globals.h>
#include <normal/cache/SegmentKey.h>
#include <normal/connector/MiniCatalogue.h>

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
  validPartitions_ = (!predicate_) ? getPartitioningScheme()->partitions() : getValidPartitions(predicate_);

  // construct physical operators
  auto mode = getMode();
  switch (mode->id()) {
    case plan::operator_::mode::FullPullup: return toOperatorsFullPullup(NumRanges);
    case plan::operator_::mode::FullPushdown: return toOperatorsFullPushdown(NumRanges);
    case plan::operator_::mode::PullupCaching: return toOperatorsPullupCaching(NumRanges);
    case plan::operator_::mode::HybridCaching: return toOperatorsHybridCaching(NumRanges);
    case plan::operator_::mode::HybridCachingLast: return toOperatorsHybridCachingLast(NumRanges);
    default:
      throw std::runtime_error(fmt::format("Unrecognized mode: '{}'", mode->toString()));
  }
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>
S3SelectScanLogicalOperator::toOperatorsFullPullup(int numRanges) {
  auto miniCatalogue = normal::connector::defaultMiniCatalogue;
  auto allColumnNames = miniCatalogue->getColumnsOfTable(getName());

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

  auto allNeededColumnNames = std::make_shared<std::vector<std::string>>();
  allNeededColumnNames->insert(allNeededColumnNames->end(), projectedColumnNames_->begin(), projectedColumnNames_->end());
  allNeededColumnNames->insert(allNeededColumnNames->end(), predicateColumnNames->begin(), predicateColumnNames->end());
  auto columnNameSet = std::make_shared<std::set<std::string>>(allNeededColumnNames->begin(), allNeededColumnNames->end());
  allNeededColumnNames->assign(columnNameSet->begin(), columnNameSet->end());

  /**
   * For each range of each partition, create a s3scan (and a filter if needed)
   */
  streamOutPhysicalOperators_ = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  auto queryId = getQueryId();

  for (const auto &partition: *validPartitions_) {
    auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);
    auto s3Bucket = s3Partition->getBucket();
    auto s3Object = s3Partition->getObject();
    auto numBytes = s3Partition->getNumBytes();
    auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, numRanges);

    int rangeId = 0;
    for (const auto &scanRange: scanRanges) {
      // S3Scan
      auto scanOp = S3SelectScan::make(
              "s3scan - " + s3Partition->getBucket() + "/" + s3Object + "-" + std::to_string(rangeId),
              s3Partition->getBucket(),
              s3Object,
              "",
              *allNeededColumnNames,
              scanRange.first,
              scanRange.second,
              S3SelectCSVParseOptions(",", "\n"),
              DefaultS3Client,
              true,
              false,
              queryId,
              std::pair<bool, std::vector<std::string>>(false, *allColumnNames));
      operators->emplace_back(scanOp);

      // Filter if it has filterPredicate
      if (predicate_) {
        auto filter = filter::Filter::make(
                fmt::format("filter-{}/{}-{}", s3Bucket, s3Object, rangeId),
                filterPredicate,
                queryId);
        operators->emplace_back(filter);

        scanOp->produce(filter);
        filter->consume(scanOp);
        streamOutPhysicalOperators_->emplace_back(filter);
      } else {
        streamOutPhysicalOperators_->emplace_back(scanOp);
      }

      rangeId++;
    }
  }

  return operators;
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>
S3SelectScanLogicalOperator::toOperatorsFullPushdown(int numRanges) {

  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  auto filterSql = genFilterSql();
  auto queryId = getQueryId();

  for (const auto &partition: *validPartitions_) {
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
              DefaultS3Client,
              true,
              false,
              queryId);
      operators->emplace_back(scanOp);
      rangeId++;
    }
  }

  return operators;
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>
S3SelectScanLogicalOperator::toOperatorsPullupCaching(int numRanges) {
  auto miniCatalogue = normal::connector::defaultMiniCatalogue;
  auto allColumnNames = miniCatalogue->getColumnsOfTable(getName());

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
  auto queryId = getQueryId();

  for (const auto &partition: *validPartitions_) {
    auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);
    auto s3Bucket = s3Partition->getBucket();
    auto s3Object = s3Partition->getObject();
    auto numBytes = s3Partition->getNumBytes();
    auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, numRanges);

    int rangeId = 0;
    for (const auto &scanRange: scanRanges) {
      // weighted segment keys
      auto weightedSegmentKeys = std::make_shared<std::vector<std::shared_ptr<normal::cache::SegmentKey>>>();
      for (auto const &projectedColumnName: *projectedColumnNames_) {
        weightedSegmentKeys->emplace_back(normal::cache::SegmentKey::make(s3Partition, projectedColumnName,
                                                                          SegmentRange::make(scanRange.first, scanRange.second)));
      }
      for (auto const &predicateColumnName: *predicateColumnNames) {
        if (std::find(projectedColumnNames_->begin(), projectedColumnNames_->end(), predicateColumnName) ==
          projectedColumnNames_->end()) {
          weightedSegmentKeys->emplace_back(normal::cache::SegmentKey::make(s3Partition, predicateColumnName,
                                                                            SegmentRange::make(scanRange.first,
                                                                                               scanRange.second)));
        }
      }

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
              DefaultS3Client,
              false,
              true,
              queryId,
              std::pair<bool, std::vector<std::string>>(false, *allColumnNames));
      operators->emplace_back(scanOp);

      // CacheLoad
      auto cacheLoad = pushdown::cache::CacheLoad::make(
              fmt::format("cacheLoad-{}/{}-{}", s3Bucket, s3Object, rangeId),
              *projectedColumnNames_,
              *predicateColumnNames,
              partition,
              scanRange.first,
              scanRange.second,
              true,
              queryId);
      operators->emplace_back(cacheLoad);

      // Merge
      auto merge = merge::Merge::make(
              fmt::format("merge-{}/{}-{}", s3Bucket, s3Object, rangeId), queryId);
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
                filterPredicate,
                queryId,
                weightedSegmentKeys);
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
  auto queryId = getQueryId();

  for (const auto &partition: *validPartitions_) {
    auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);
    auto s3Bucket = s3Partition->getBucket();
    auto s3Object = s3Partition->getObject();
    auto numBytes = s3Partition->getNumBytes();
    auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, numRanges);

    int rangeId = 0;
    for (const auto &scanRange: scanRanges) {
      // weighted segment keys
      auto weightedSegmentKeys = std::make_shared<std::vector<std::shared_ptr<normal::cache::SegmentKey>>>();
      for (auto const &projectedColumnName: *projectedColumnNames_) {
        weightedSegmentKeys->emplace_back(normal::cache::SegmentKey::make(s3Partition, projectedColumnName,
                                                                          SegmentRange::make(scanRange.first, scanRange.second)));
      }
      for (auto const &predicateColumnName: *predicateColumnNames) {
        if (std::find(projectedColumnNames_->begin(), projectedColumnNames_->end(), predicateColumnName) ==
            projectedColumnNames_->end()) {
          weightedSegmentKeys->emplace_back(normal::cache::SegmentKey::make(s3Partition, predicateColumnName,
                                                                            SegmentRange::make(scanRange.first,
                                                                                               scanRange.second)));
        }
      }

      // cacheLoad
      auto cacheLoad = pushdown::cache::CacheLoad::make(
              fmt::format("cacheLoad-{}/{}-{}", s3Bucket, s3Object, rangeId),
              *projectedColumnNames_,
              *predicateColumnNames,
              partition,
              scanRange.first,
              scanRange.second,
              true,
              queryId);
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
              DefaultS3Client,
              false,
              true,
              queryId);
      operators->emplace_back(s3Scan);

      // merge1
      auto merge1 = merge::Merge::make(
              fmt::format("merge1-{}/{}-{}", s3Bucket, s3Object, rangeId), queryId);
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
                filterPredicate,
                queryId,
                weightedSegmentKeys);
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
              DefaultS3Client,
              false,
              false,
              queryId,
              std::pair<bool, std::vector<std::string>>(true, std::vector<std::string>()),
              weightedSegmentKeys);
      operators->emplace_back(s3Select);

      // merge2
      auto merge2 = merge::Merge::make(
              fmt::format("merge2-{}/{}-{}", s3Bucket, s3Object, rangeId), queryId);
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
              fmt::format("project-{}/{}-{}", s3Bucket, s3Object, rangeId), *projectExpressions, queryId);
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

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>>
S3SelectScanLogicalOperator::toOperatorsHybridCachingLast(int numRanges) {

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
   * a CacheLoad, a S3Scan which is to pull up segments to cache (but not used in the current query), a Filter if needed
   * a S3Select, a Merge for filtered hit segments + S3Select result
   * a Project to make all tupleSets have the same schema
   */
  streamOutPhysicalOperators_ = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();
  auto queryId = getQueryId();

  for (const auto &partition: *validPartitions_) {
    auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);
    auto s3Bucket = s3Partition->getBucket();
    auto s3Object = s3Partition->getObject();
    auto numBytes = s3Partition->getNumBytes();
    auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, numRanges);

    int rangeId = 0;
    for (const auto &scanRange: scanRanges) {

      // cacheLoad
      auto cacheLoad = pushdown::cache::CacheLoad::make(
              fmt::format("cacheLoad-{}/{}-{}", s3Bucket, s3Object, rangeId),
              *projectedColumnNames_,
              *predicateColumnNames,
              partition,
              scanRange.first,
              scanRange.second,
              false,
              queryId);
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
              DefaultS3Client,
              false,
              true,
              queryId);
      operators->emplace_back(s3Scan);

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
              DefaultS3Client,
              false,
              false,
              queryId);
      operators->emplace_back(s3Select);

      // merge
      auto merge = merge::Merge::make(
              fmt::format("merge-{}/{}-{}", s3Bucket, s3Object, rangeId), queryId);
      operators->emplace_back(merge);

      // wire up cacheLoad, s3Scan, s3Select and merge
      cacheLoad->setMissOperatorToCache(s3Scan);
      s3Scan->consume(cacheLoad);

      cacheLoad->setMissOperatorToPushdown(s3Select);
      s3Select->consume(cacheLoad);

      s3Select->produce(merge);
      merge->setRightProducer(s3Select);

      // filter if it has filterPredicate
      if (predicate_) {
        auto filter = filter::Filter::make(
                fmt::format("filter-{}/{}-{}", s3Bucket, s3Object, rangeId),
                filterPredicate,
                queryId);
        operators->emplace_back(filter);

        // wire up cacheLoad, filter and merge
        cacheLoad->setHitOperator(filter);
        filter->consume(cacheLoad);

        filter->produce(merge);
        merge->setLeftProducer(filter);
      } else {
        // wire cacheLoad and merge
        cacheLoad->setHitOperator(merge);
        merge->setLeftProducer(cacheLoad);
      }

      // project
      auto projectExpressions = std::make_shared<std::vector<std::shared_ptr<expression::gandiva::Expression>>>();
      for (auto const &projectedColumnName: *projectedColumnNames_) {
        projectExpressions->emplace_back(expression::gandiva::col(projectedColumnName));
      }
      auto project = std::make_shared<Project>(
              fmt::format("project-{}/{}-{}", s3Bucket, s3Object, rangeId), *projectExpressions, queryId);
      operators->emplace_back(project);

      // wire up merge and project
      merge->produce(project);
      project->consume(merge);

      // project is the stream-out physical operator
      streamOutPhysicalOperators_->emplace_back(project);

      rangeId++;
    }
  }

  return operators;
}

// Extract the segmentKeys that are to be scanned in this query, this information is needed for the
// Belady caching algorithm
std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> S3SelectScanLogicalOperator::extractSegmentKeys() {
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

  auto allColumnNames = std::make_shared<std::vector<std::string>>();
  allColumnNames->insert(allColumnNames->end(), projectedColumnNames_->begin(), projectedColumnNames_->end());
  allColumnNames->insert(allColumnNames->end(), predicateColumnNames->begin(), predicateColumnNames->end());
  auto columnNameSet = std::make_shared<std::set<std::string>>(allColumnNames->begin(), allColumnNames->end());
  // use set to remove duplicates
  allColumnNames->assign(columnNameSet->begin(), columnNameSet->end());

  /**
   * Examine range of each partition, and create corresponding segment keys
   */
  auto involvedSegmentKeys = std::make_shared<std::vector<std::shared_ptr<normal::cache::SegmentKey>>>();

  for (const auto &partition: *validPartitions_) {
    auto s3Partition = std::static_pointer_cast<S3SelectPartition>(partition);
    auto numBytes = s3Partition->getNumBytes();
    auto segmentRange = SegmentRange::make(0, numBytes);
    for (auto column : *allColumnNames) {
      auto segmentKey = SegmentKey::make(s3Partition, column, segmentRange);
      involvedSegmentKeys->emplace_back(segmentKey);
    }

  }

  return involvedSegmentKeys;
}

//    int rangeId = 0;
//    for (const auto &scanRange: scanRanges) {
//      // S3Scan
//      auto scanOp = S3SelectScan::make(
//              "s3scan - " + s3Partition->getBucket() + "/" + s3Object + "-" + std::to_string(rangeId),
//              s3Partition->getBucket(),
//              s3Object,
//              "",
//              *allColumnNames,
//              scanRange.first,
//              scanRange.second,
//              S3SelectCSVParseOptions(",", "\n"),
//              DefaultS3Client,
//              true,
//              false);
//      operators->emplace_back(scanOp);
//
//      // Filter if it has filterPredicate
//      if (predicate_) {
//        auto filter = filter::Filter::make(
//                fmt::format("filter-{}/{}-{}", s3Bucket, s3Object, rangeId),
//                filterPredicate);
//        operators->emplace_back(filter);
//        streamOutPhysicalOperators_->emplace_back(filter);
//
//        scanOp->produce(filter);
//        filter->consume(scanOp);
//      } else {
//        streamOutPhysicalOperators_->emplace_back(scanOp);
//      }
//
//      rangeId++;
//    }
//  }