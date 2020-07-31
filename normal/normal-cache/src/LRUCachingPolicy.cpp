//
// Created by matt on 2/6/20.
//

#include "normal/cache/LRUCachingPolicy.h"

using namespace normal::cache;

LRUCachingPolicy::LRUCachingPolicy(size_t maxSize) :
  maxSize_(maxSize),
  freeSize_(maxSize) {}

std::shared_ptr<LRUCachingPolicy> LRUCachingPolicy::make() {
  return std::make_shared<LRUCachingPolicy>(std::numeric_limits<size_t>::max());
}

std::shared_ptr<LRUCachingPolicy> LRUCachingPolicy::make(size_t maxSize) {
  return std::make_shared<LRUCachingPolicy>(maxSize);
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


