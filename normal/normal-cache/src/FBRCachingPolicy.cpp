//
// Created by Yifei Yang on 8/3/20.
//

#include "normal/cache/FBRCachingPolicy.h"

using namespace normal::cache;

bool lessFrequent (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  return key1->getMetadata()->hitNum() > key2->getMetadata()->hitNum();
}

FBRCachingPolicy::FBRCachingPolicy(size_t maxSize) :
        CachingPolicy(maxSize) {}

std::shared_ptr<FBRCachingPolicy> FBRCachingPolicy::make(size_t maxSize) {
  return std::make_shared<FBRCachingPolicy>(maxSize);
}


void FBRCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &key) {
  auto keyEntry = keySet_.find(key);
  if (keyEntry != keySet_.end()) {
    keyEntry->get()->getMetadata()->incHitNum();
    std::make_heap(usageHeap_.begin(), usageHeap_.end(), lessFrequent);
  }
}

void FBRCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  erase(key);
}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>
FBRCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
  auto removableKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  auto segmentSize = key->getMetadata()->size();

  if (maxSize_ < segmentSize) {
    return std::nullopt;
  }

  while (freeSize_ < segmentSize) {
    std::pop_heap(usageHeap_.begin(), usageHeap_.end(), lessFrequent);
    auto removableKey = usageHeap_.back();
    removableKeys->emplace_back(removableKey);
    eraseFBR();
    freeSize_ += removableKey->getMetadata()->size();
  }
  usageHeap_.emplace_back(key);
  std::push_heap(usageHeap_.begin(), usageHeap_.end(), lessFrequent);
  keySet_.emplace(key);
  freeSize_ -= segmentSize;

  return std::optional(removableKeys);
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
FBRCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  return segmentKeys;
}

void FBRCachingPolicy::erase(const std::shared_ptr<SegmentKey> &key) {
  keySet_.erase(key);
  for (auto it = usageHeap_.begin(); it != usageHeap_.end();) {
    if (it->get()->operator==(*key)) {
      it = usageHeap_.erase(it);
      std::make_heap(usageHeap_.begin(), usageHeap_.end(), lessFrequent);
      return;
    }
  }
}

void FBRCachingPolicy::eraseFBR() {
  keySet_.erase(usageHeap_.back());
  usageHeap_.pop_back();
}
