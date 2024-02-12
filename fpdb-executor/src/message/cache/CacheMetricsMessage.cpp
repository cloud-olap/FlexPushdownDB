//
// Created by Yifei Yang on 11/2/20.
//

#include <fpdb/executor/message/cache/CacheMetricsMessage.h>

namespace fpdb::executor::message {

CacheMetricsMessage::CacheMetricsMessage(size_t hitNum, size_t missNum, size_t shardHitNum, size_t shardMissNum, const std::string &sender) :
  Message(CACHE_METRICS, sender),
  hitNum_(hitNum),
  missNum_(missNum),
  shardHitNum_(shardHitNum),
  shardMissNum_(shardMissNum) {}

std::shared_ptr<CacheMetricsMessage>
CacheMetricsMessage::make(size_t hitNum, size_t missNum, size_t shardHitNum, size_t shardMissNum, const std::string &sender) {
  return std::make_shared<CacheMetricsMessage>(hitNum, missNum, shardHitNum, shardMissNum, sender);
}

std::string CacheMetricsMessage::getTypeString() const {
  return "CacheMetricsMessage";
}

size_t CacheMetricsMessage::getHitNum() const {
  return hitNum_;
}

size_t CacheMetricsMessage::getMissNum() const {
  return missNum_;
}

size_t CacheMetricsMessage::getShardHitNum() const {
  return shardHitNum_;
}

size_t CacheMetricsMessage::getShardMissNum() const {
  return shardMissNum_;
}

}
