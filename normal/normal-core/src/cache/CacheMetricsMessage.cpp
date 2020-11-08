//
// Created by Yifei Yang on 11/2/20.
//

#include "normal/core/cache/CacheMetricsMessage.h"

using namespace normal::core::cache;

CacheMetricsMessage::CacheMetricsMessage(size_t hitNum, size_t missNum, const std::string &sender) :
  Message("CacheMetricsMessage", sender),
  hitNum_(hitNum),
  missNum_(missNum) {}

std::shared_ptr<CacheMetricsMessage>
CacheMetricsMessage::make(size_t hitNum, size_t missNum, const std::string &sender) {
  return std::make_shared<CacheMetricsMessage>(hitNum, missNum, sender);
}

size_t CacheMetricsMessage::getHitNum() const {
  return hitNum_;
}

size_t CacheMetricsMessage::getMissNum() const {
  return missNum_;
}
