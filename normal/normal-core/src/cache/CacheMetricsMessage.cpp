//
// Created by Yifei Yang on 11/2/20.
//

#include "normal/core/cache/CacheMetricsMessage.h"

using namespace normal::core::cache;

CacheMetricsMessage::CacheMetricsMessage(size_t hitBytes, size_t missBytes, const std::string &sender) :
  Message("CacheMetricsMessage", sender),
  hitBytes_(hitBytes),
  missBytes_(missBytes) {}

std::shared_ptr<CacheMetricsMessage>
CacheMetricsMessage::make(size_t hitBytes, size_t missBytes, const std::string &sender) {
  return std::make_shared<CacheMetricsMessage>(hitBytes, missBytes, sender);
}

size_t CacheMetricsMessage::getHitBytes() const {
  return hitBytes_;
}

size_t CacheMetricsMessage::getMissBytes() const {
  return missBytes_;
}
