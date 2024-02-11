//
// Created by matt on 2/6/20.
//

#include <sstream>
#include <utility>
#include "normal/cache/LRUCachingPolicy.h"
#include <normal/plan/mode/Modes.h>

using namespace normal::cache;

LRUCachingPolicy::LRUCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) :
  CachingPolicy(maxSize, std::move(mode)) {}

std::shared_ptr<LRUCachingPolicy> LRUCachingPolicy::make() {
  return std::make_shared<LRUCachingPolicy>(std::numeric_limits<size_t>::max(),
                                            normal::plan::operator_::mode::Modes::pullupCachingMode());
}

std::shared_ptr<LRUCachingPolicy> LRUCachingPolicy::make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) {
  return std::make_shared<LRUCachingPolicy>(maxSize, mode);
}

void LRUCachingPolicy::erase(const std::shared_ptr<SegmentKey> &key) {
  auto keyIndexEntry = keyIndexMap_.find(key);
  if (keyIndexEntry != keyIndexMap_.end()) {
	usageQueue_.erase(keyIndexEntry->second);
  }
  keyIndexMap_.erase(key);
}

void LRUCachingPolicy::eraseLRU(){
  keyIndexMap_.erase(usageQueue_.back());
  usageQueue_.pop_back();
}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>
LRUCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
  auto removableKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  auto segmentSize = key->getMetadata()->size();

  if (maxSize_ < segmentSize) {
    return std::nullopt;
  }

  while (freeSize_ < segmentSize) {
    auto removableKey = usageQueue_.back();
    removableKeys->emplace_back(removableKey);
    eraseLRU();
    freeSize_ += removableKey->getMetadata()->size();
  }
  usageQueue_.emplace_front(key);
  keyIndexMap_.emplace(key, usageQueue_.begin());
  freeSize_ -= segmentSize;

  return std::optional(removableKeys);
}

void LRUCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &key) {
  auto keyIndexEntry = keyIndexMap_.find(key);
  if (keyIndexEntry != keyIndexMap_.end()) {
	usageQueue_.splice(usageQueue_.begin(), usageQueue_, keyIndexEntry->second);
  }
}

void LRUCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  erase(key);
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
LRUCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  /**
   * LRU always return all miss segment keys, for they are all newest
   */
  return segmentKeys;
}

std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>
LRUCachingPolicy::getKeysetInCachePolicy() {
  auto keysetInCachePolicy = std::make_shared<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
  keysetInCachePolicy->insert(usageQueue_.begin(), usageQueue_.end());
  return keysetInCachePolicy;
}

std::string LRUCachingPolicy::showCurrentLayout() {
  std::stringstream ss;
  for (auto const &segmentKey: usageQueue_) {
    ss << segmentKey->toString() << std::endl;
  }
  return ss.str();
}

CachingPolicyId LRUCachingPolicy::id() {
  return LRU;
}

std::string LRUCachingPolicy::toString() {
  return "LRU";
}

void LRUCachingPolicy::onNewQuery() {

}


