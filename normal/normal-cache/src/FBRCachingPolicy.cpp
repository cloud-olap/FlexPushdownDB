//
// Created by Yifei Yang on 8/3/20.
//

#include <sstream>
#include <fmt/format.h>
#include "normal/cache/FBRCachingPolicy.h"
#include <algorithm>

using namespace normal::cache;

bool lessValue (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
//  return (key1->getMetadata()->hitNum() / key1->getMetadata()->size())
//       < (key2->getMetadata()->hitNum() / key2->getMetadata()->size());
  return (key1->getMetadata()->hitNum())
         < (key2->getMetadata()->hitNum());
}

bool lessEstimateValue (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
//  return (key1->getMetadata()->hitNum() / key1->getMetadata()->estimateSize())
//       < (key2->getMetadata()->hitNum() / key2->getMetadata()->estimateSize());
  return (key1->getMetadata()->hitNum())
         < (key2->getMetadata()->hitNum());
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
  } else {
    keySet_.emplace(key);
  }
}

void FBRCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  erase(key);
}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>
FBRCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
  auto removableKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  // decide whether to cache
  std::shared_ptr<SegmentKey> realKey;
  auto keyEntry = keySet_.find(key);
  if (keyEntry != keySet_.end()) {
    realKey = *keyEntry;
    if (realKey->getMetadata()->size() == 0) {
      realKey->getMetadata()->setSize(key->getMetadata()->size());
    }
  } else {
    throw std::runtime_error("onStore: Key should exist in keySet_");
  }

  auto segmentSize = realKey->getMetadata()->size();
  if (maxSize_ < segmentSize) {
    removeEstimateCachingDecision(realKey);
    return std::nullopt;
  }

  std::sort(keysInCache_.begin(), keysInCache_.end(), lessValue);
  int heapIndex = 0;
  size_t tmpFreeSize = freeSize_;
  while (tmpFreeSize < segmentSize) {
    auto removableKey = keysInCache_[heapIndex];
    if (lessValue(removableKey, realKey)) {
      removableKeys->emplace_back(removableKey);
      tmpFreeSize += removableKey->getMetadata()->size();
      ++heapIndex;
    } else {
      removeEstimateCachingDecision(realKey);
      return std::nullopt;
    }
  }

  // remove
  if (heapIndex > 0) {
    keysInCache_.erase(keysInCache_.begin(), keysInCache_.begin() + heapIndex);
    freeSize_ = tmpFreeSize;
  }

  // bring in
  keysInCache_.emplace_back(realKey);
  freeSize_ -= segmentSize;

  removeEstimateCachingDecision(realKey);
  return std::optional(removableKeys);
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
FBRCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  auto keysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  // FIXME: an estimation here, if freeSize_ >= c * maxSize_, we try to cache all segments
  //  Because we cannot know the size of segmentData before bringing it back
  if (freeSize_ >= maxSize_ * 0.1 && freeSize_ >= 100*1024*1024) {
    // keys have been added to keySet_ in onLoad() before
//    for (const auto &key: *segmentKeys) {
//      auto keyEntry = keySet_.find(key);
//      if (keyEntry != keySet_.end()) {
//        (*keyEntry)->getMetadata()->incHitNum();
//      } else {
//        keySet_.emplace(key);
//      }
//    }
    return segmentKeys;
  }

  // estimate whether to cache
  for (auto key: *segmentKeys) {
    std::shared_ptr<SegmentKey> realKey;
    auto keyEntry = keySet_.find(key);
    // keys have been added to keySet_ in onLoad() before
    if (keyEntry != keySet_.end()) {
      realKey = *keyEntry;
//      realKey->getMetadata()->incHitNum();
    } else {
//      realKey = key;
//      keySet_.emplace(realKey);
      throw std::runtime_error("onToCache: Key should exist in keySet_");
    }

    // try to find one lower-value unused key in cache
    for (const auto &keyInCache: keysInCache_) {
      if (lessEstimateValue(keyInCache, realKey) && keysToReplace_.find(keyInCache) == keysToReplace_.end()) {
        keysToCache->emplace_back(realKey);
        addEstimateCachingDecision(realKey, keyInCache);
        break;
      }
    }
  }

  return keysToCache;
}

void FBRCachingPolicy::erase(const std::shared_ptr<SegmentKey> &key) {
  keysInCache_.erase(std::find(keysInCache_.begin(), keysInCache_.end(), key));
}

long FBRCachingPolicy::value(std::shared_ptr<SegmentMetadata> metadata) {
  return metadata->hitNum() / metadata->size();
}

void FBRCachingPolicy::addEstimateCachingDecision(const std::shared_ptr<SegmentKey> &in,
                                                  const std::shared_ptr<SegmentKey> &out) {
  keysToReplace_.emplace(out);
  estimateCachingDecisions_.emplace(in, out);
}

void FBRCachingPolicy::removeEstimateCachingDecision(const std::shared_ptr<SegmentKey> &in) {
  auto keysToReplaceEntry = estimateCachingDecisions_.find(in);
  if (keysToReplaceEntry != estimateCachingDecisions_.end()) {
    keysToReplace_.erase(keysToReplaceEntry->second);
    estimateCachingDecisions_.erase(in);
  }
}

std::string FBRCachingPolicy::showCurrentLayout() {
  std::stringstream ss;
  ss << "Total numbers: " << keysInCache_.size() << std::endl;
  for (auto const &segmentKey: keysInCache_) {
    ss << fmt::format("Key: {};\tHitnum: {}\tSize: {}", segmentKey->toString(), segmentKey->getMetadata()->hitNum(), segmentKey->getMetadata()->size()) << std::endl;
  }
  ss << "Max size: " << maxSize_ << std::endl;
  ss << "Free size: " << freeSize_ << std::endl;
  return ss.str();
}
