//
// Created by matt on 8/7/20.
//

#include <fpdb/executor/physical/cache/CacheLoadPOp.h>
#include <fpdb/executor/physical/cache/CacheHelper.h>
#include <fpdb/executor/message/ScanMessage.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/executor/message/cache/CacheMetricsMessage.h>
#include <fpdb/catalogue/obj-store/s3/S3Connector.h>
#include <fpdb/util/Util.h>
#include <utility>

using namespace fpdb::util;

namespace fpdb::executor::physical::cache {

CacheLoadPOp::CacheLoadPOp(const std::string &name,
                           const std::vector<std::string> &,
                           int nodeId,
                           const std::vector<std::string> &predicateColumnNames,
                           const std::vector<std::set<std::string>> &projectColumnGroups,
                           const std::vector<std::string> &allColumnNames,
                           const std::shared_ptr<Partition> &partition,
                           int64_t startOffset,
                           int64_t finishOffset,
                           const std::optional<std::shared_ptr<ObjStoreConnector>> &objStoreConnector) :
  PhysicalOp(name, CACHE_LOAD, union_(projectColumnGroups), nodeId),
  predicateColumnNames_(predicateColumnNames),
  projectColumnGroups_(projectColumnGroups),
  allColumnNames_(allColumnNames),
  partition_(partition),
  startOffset_(startOffset),
  finishOffset_(finishOffset),
  objStoreConnector_(objStoreConnector) {}

std::string CacheLoadPOp::getTypeString() const {
  return "CacheLoadPOp";
}

const std::shared_ptr<Partition> &CacheLoadPOp::getPartition() const {
  return partition_;
}

void CacheLoadPOp::setHitOperator(const std::shared_ptr<PhysicalOp> &op) {
  this->hitOperatorName_ = op->name();
  this->produce(op);
}

void CacheLoadPOp::setMissOperatorToCache(const std::shared_ptr<PhysicalOp> &op) {
  this->missOperatorToCacheName_ = op->name();
  this->produce(op);
}

void CacheLoadPOp::setMissOperatorToPushdown(const std::shared_ptr<PhysicalOp> &op) {
  this->missOperatorToPushdownName_ = op->name();
  this->produce(op);
}

void CacheLoadPOp::enableBitmapPushdown() {
  isBitmapPushdownEnabled_ = true;
}

void CacheLoadPOp::onReceive(const Envelope &message) {
  if (message.message().type() == MessageType::START) {
	  this->onStart();
  } else if (message.message().type() == MessageType::LOAD_RESPONSE) {
    auto loadResponseMessage = dynamic_cast<const LoadResponseMessage &>(message.message());
    this->onCacheLoadResponse(loadResponseMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.message().getTypeString());
  }
}

void CacheLoadPOp::onStart() {
  /**
   * We always have hitOperator_ and missOperatorToCache_
   * In hybrid caching, we also have missOperatorToPushdown_; in pullup caching, we don't
   */

  if (!hitOperatorName_.has_value()) {
    ctx()->notifyError("Hit consumer not set ");
    return;
  }

  if (!missOperatorToCacheName_.has_value()) {
    ctx()->notifyError("Miss caching consumer not set");
    return;
  }

  if (missOperatorToPushdownName_.has_value()) {
    SPDLOG_DEBUG("Starting operator  |  name: '{}', hitOperator: '{}', missOperatorToCache: '{}', missOperatorToPushdown: '{}'",
                 this->name(),
                 *hitOperatorName_,
                 *missOperatorToCacheName_,
                 *missOperatorToPushdownName_);
    if (!objStoreConnector_.has_value()) {
      ctx()->notifyError("Object store connector not set for hybrid execution");
      return;
    }
  } else {
    SPDLOG_DEBUG("Starting operator  |  name: '{}', hitOperator: '{}', missOperatorToCache: '{}'",
                 this->name(),
                 *hitOperatorName_,
                 *missOperatorToCacheName_);
  }

  requestLoadSegmentsFromCache();
}

void CacheLoadPOp::requestLoadSegmentsFromCache() {
  CacheHelper::requestLoadSegmentsFromCache(allColumnNames_, partition_, startOffset_, finishOffset_, name(), ctx());
}

void CacheLoadPOp::onCacheLoadResponse(const LoadResponseMessage &message) {
  auto hitSegmentMap = message.getSegments();
  SPDLOG_DEBUG("Loaded segments from cache  |  numRequested: {}, numHit: {}", allColumnNames_.size(), hitSegmentMap.size());

  // Final variables to make messages
  std::vector<std::shared_ptr<Column>> hitColumns;
  std::vector<std::string> missCachingColumnNames;
  std::vector<std::string> missPushdownColumnNames;

  /**
   * 1. Get hitColumnNames, missCachingColumnNames, and missPushdownProjectColumnNameSet
   */
  // Get hitColumnNames and missCachingColumnNames, and make hitMissCachingColumnNameSet
  std::set<std::string> hitMissCachingColumnNameSet;
  for (const auto &hitSegmentIt: hitSegmentMap) {
    hitColumns.emplace_back(hitSegmentIt.second->getColumn());
    hitMissCachingColumnNameSet.emplace(hitSegmentIt.first->getColumnName());
  }
  for (const auto &segmentKey: message.getSegmentKeysToCache()) {
    missCachingColumnNames.emplace_back(segmentKey->getColumnName());
    hitMissCachingColumnNameSet.emplace(segmentKey->getColumnName());
  }

  // Get missPushdownColumnNames for hybrid execution, using projectColumnGroups
  std::set<std::string> missPushdownProjectColumnNameSet;
  bool coverSomeProjectColumnGroup = false;
  if (missOperatorToPushdownName_.has_value()) {
    // For each projectColumnGroup, check if it's covered by hitMissCachingColumnNameSet, if true then we can execute
    // the corresponding part using cache, if not we push down that part
    for (const auto &projectColumnGroup: projectColumnGroups_) {
      if (!isSubSet(projectColumnGroup, hitMissCachingColumnNameSet)) {
        missPushdownProjectColumnNameSet.insert(projectColumnGroup.begin(), projectColumnGroup.end());
      } else {
        coverSomeProjectColumnGroup = true;
      }
    }
  }

  /**
   * 2. Check whether missCachingColumns are needed after loaded from store
   * For caching-only: true
   * For hybrid:
   *   True when "hitMissCachingColumnNameSet covers all predicateColumnNames"
   *     and     "hitMissCachingColumnNameSet covers at lease one projectColumnGroup"
   *   Exception for Airmettle:
   *   True when "hitMissCachingColumnNameSet covers allColumnNames", because
   *     Airmettle doesn't support intra-partition hybrid execution due to that it doesn't preserve order
   */
  bool coverAllPredicateColumns = isSubSet(
          std::set<std::string>(predicateColumnNames_.begin(), predicateColumnNames_.end()),
          hitMissCachingColumnNameSet);

  bool cachingColumnsNeeded;
  if (!missOperatorToPushdownName_.has_value()) {
    cachingColumnsNeeded = true;
  } else {
    if ((*objStoreConnector_)->getStoreType() == ObjStoreType::S3 &&
      std::static_pointer_cast<S3Connector>(*objStoreConnector_)
              ->getAwsClient()
              ->getAwsConfig()
              ->getS3ClientType() == S3ClientType::AIRMETTLE) {
      cachingColumnsNeeded = (hitMissCachingColumnNameSet.size() == allColumnNames_.size());
    } else {
      cachingColumnsNeeded = coverAllPredicateColumns && coverSomeProjectColumnGroup;
    }
  }

  /**
   * 3. Send messages to consumers
   */
  // To hitOperator
  auto hitTupleSet = TupleSet::make(hitColumns);
  auto hitMessage = std::make_shared<TupleSetMessage>(hitTupleSet, this->name());
  ctx()->send(hitMessage, *hitOperatorName_);

  // To missOperatorToCache
  auto missCachingMessage = std::make_shared<ScanMessage>(std::vector<std::string>{},
                                                          missCachingColumnNames,
                                                          this->name(),
                                                          cachingColumnsNeeded);
  ctx()->send(missCachingMessage, *missOperatorToCacheName_);

  // To missOperatorToPushdown, need to distinguish between S3 and fpdb-store, and whether bitmap pushdown is enabled
  if (missOperatorToPushdownName_.has_value()) {
    std::vector<std::string> scanColumnNames;   // columns to scan at fpdb-store may be different to columns returned
    switch ((*objStoreConnector_)->getStoreType()) {
      case ObjStoreType::S3: {
        missPushdownColumnNames = cachingColumnsNeeded ?
                                  std::vector<std::string>(missPushdownProjectColumnNameSet.begin(),
                                                           missPushdownProjectColumnNameSet.end()) :
                                  projectColumnNames_;
        break;
      }
      case ObjStoreType::FPDB_STORE: {
        if (!isBitmapPushdownEnabled_) {
          missPushdownColumnNames = cachingColumnsNeeded ?
                                    std::vector<std::string>(missPushdownProjectColumnNameSet.begin(),
                                                             missPushdownProjectColumnNameSet.end()) :
                                    projectColumnNames_;
          scanColumnNames = missPushdownColumnNames;
          // Need to add predicateColumnNames for fpdb-store because we push query plan rather than sql
          if (!missPushdownColumnNames.empty()) {
            scanColumnNames = union_(missPushdownColumnNames, predicateColumnNames_);
          }
        } else {
          missPushdownColumnNames = std::vector<std::string>(missPushdownProjectColumnNameSet.begin(),
                                                             missPushdownProjectColumnNameSet.end());
          scanColumnNames = missPushdownColumnNames;
          // If bitmap cannot be constructed at compute side, need to add predicateColumnNames
          if (!coverAllPredicateColumns) {
            scanColumnNames = union_(missPushdownColumnNames, predicateColumnNames_);
          }
        }
        break;
      }
      default: {
        ctx()->notifyError("Unknown object store type");
      }
    }

    auto missPushdownMessage = std::make_shared<ScanMessage>(scanColumnNames,
                                                             missPushdownColumnNames,
                                                             this->name(),
                                                             true);
    ctx()->send(missPushdownMessage, *missOperatorToPushdownName_);
  }

  /**
   * 4. Send cache metrics to segmentCacheActor and complete
   */
  size_t hitNum = hitColumns.size();
  size_t missNum = allColumnNames_.size() - hitNum;

  // If every segment is a hit, then we call it a shard hit, otherwise a shard miss
  size_t shardHitNum, shardMissNum;
  if (hitColumns.size() == allColumnNames_.size()) {
    shardHitNum = 1;
    shardMissNum = 0;
  } else {
    shardHitNum = 0;
    shardMissNum = 1;
  }
  ctx()->send(CacheMetricsMessage::make(hitNum, missNum, shardHitNum, shardMissNum, this->name()), "SegmentCache");
  ctx()->notifyComplete();
}

void CacheLoadPOp::clear() {
  // Noop
}

}
