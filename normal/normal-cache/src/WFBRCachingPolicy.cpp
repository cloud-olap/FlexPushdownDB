//
// Created by Yifei Yang on 9/10/20.
//

#include <sstream>
#include <fmt/format.h>
#include <normal/cache/WFBRCachingPolicy.h>

using namespace normal::cache;

bool WFBRCachingPolicy::lessValue(const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
//  if (key1->getMetadata()->hitNum() != key2->getMetadata()->hitNum()) {
//    return key1->getMetadata()->hitNum() < key2->getMetadata()->hitNum();
//  } else {
//    return (key1->getMetadata()->value()) < (key2->getMetadata()->value());
//  }

  return (key1->getMetadata()->value()) < (key2->getMetadata()->value());
//  return (key1->getMetadata()->value2()) < (key2->getMetadata()->value2());

//  return (key1->getMetadata()->avgValue()) < (key2->getMetadata()->avgValue());

//  auto value1 = (1.0 + key1->getMetadata()->value() / ((double) (1 + currentQueryId_))) * ((double) key1->getMetadata()->hitNum());
//  auto value2 = (1.0 + key2->getMetadata()->value() / ((double) (1 + currentQueryId_))) * ((double) key2->getMetadata()->hitNum());
//  return value1 < value2;
}

WFBRCachingPolicy::WFBRCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode):
      CachingPolicy(maxSize, mode),
      currentQueryId_(0) {}

std::shared_ptr<WFBRCachingPolicy>
WFBRCachingPolicy::make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) {
  return std::make_shared<WFBRCachingPolicy>(maxSize, mode);
}

void WFBRCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &key) {
  auto keyEntry = keySet_.find(key);
  if (keyEntry != keySet_.end()) {
    keyEntry->get()->getMetadata()->incHitNum();
  } else {
    keySet_.emplace(key);
  }
}

void WFBRCachingPolicy::onWeight(const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &weightMap, long queryId) {
  // if new query executes, clear temporary weight updated keys
  if (queryId > currentQueryId_) {
    weightUpdatedKeys_.clear();
    currentQueryId_ = queryId;
  }

  // update value using weight
  for (auto const &weightEntry: *weightMap) {
    auto segmentKey = weightEntry.first;
    auto weight = weightEntry.second;
    if (weightUpdatedKeys_.find(segmentKey) == weightUpdatedKeys_.end()) {
      std::shared_ptr<SegmentKey> realKey;
      auto keyEntry = keySet_.find(segmentKey);
      if (keyEntry != keySet_.end()) {
        realKey = *keyEntry;
        realKey->getMetadata()->addValue(weight);
      } else {
        throw std::runtime_error("onWeight: Key should exist in keySet_");
      }

      weightUpdatedKeys_.emplace(realKey);
    }
  }
}

void WFBRCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  erase(key);
}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>
WFBRCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
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

  std::sort(keysInCache_.begin(), keysInCache_.end(),
            [this](const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
                return lessValue(key1, key2);
            });
  int removeIndex = 0;
  size_t tmpFreeSize = freeSize_;
  while (tmpFreeSize < segmentSize) {
    auto removableKey = keysInCache_[removeIndex];
    if (lessValue(removableKey, realKey)) {
      removableKeys->emplace_back(removableKey);
      tmpFreeSize += removableKey->getMetadata()->size();
      ++removeIndex;
    } else {
      removeEstimateCachingDecision(realKey);
      return std::nullopt;
    }
  }

  // remove
  if (removeIndex > 0) {
    keysInCache_.erase(keysInCache_.begin(), keysInCache_.begin() + removeIndex);
    freeSize_ = tmpFreeSize;
  }

  // bring in
  keysInCache_.emplace_back(realKey);
  freeSize_ -= segmentSize;

  removeEstimateCachingDecision(realKey);
  return std::optional(removableKeys);
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
WFBRCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  if (mode_->id() == normal::plan::operator_::mode::ModeId::PullupCaching) {
    return segmentKeys;
  }

  auto keysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  // FIXME: an estimation here, if freeSize_ >= c * maxSize_, we try to cache all segments
  //  Because we cannot know the size of segmentData before bringing it back
  if (freeSize_ >= maxSize_ * 0.1 && freeSize_ >= 100*1024*1024) {
    return segmentKeys;
  }

  // estimate whether to cache
  for (auto key: *segmentKeys) {
    std::shared_ptr<SegmentKey> realKey;
    auto keyEntry = keySet_.find(key);
    // keys have been added to keySet_ in onLoad() before
    if (keyEntry != keySet_.end()) {
      realKey = *keyEntry;
    } else {
      throw std::runtime_error("onToCache: Key should exist in keySet_");
    }

    // try to find one lower-value unused key in cache
    for (const auto &keyInCache: keysInCache_) {
      if (lessValue(keyInCache, realKey) && keysToReplace_.find(keyInCache) == keysToReplace_.end()) {
        keysToCache->emplace_back(realKey);
        addEstimateCachingDecision(realKey, keyInCache);
        break;
      }
    }
  }

  return keysToCache;
}

void WFBRCachingPolicy::erase(const std::shared_ptr<SegmentKey> &key) {
  keysInCache_.erase(std::find(keysInCache_.begin(), keysInCache_.end(), key));
}

void WFBRCachingPolicy::addEstimateCachingDecision(const std::shared_ptr<SegmentKey> &in,
                                                  const std::shared_ptr<SegmentKey> &out) {
  keysToReplace_.emplace(out);
  estimateCachingDecisions_.emplace(in, out);
}

void WFBRCachingPolicy::removeEstimateCachingDecision(const std::shared_ptr<SegmentKey> &in) {
  auto keysToReplaceEntry = estimateCachingDecisions_.find(in);
  if (keysToReplaceEntry != estimateCachingDecisions_.end()) {
    keysToReplace_.erase(keysToReplaceEntry->second);
    estimateCachingDecisions_.erase(in);
  }
}

std::string WFBRCachingPolicy::showCurrentLayout() {
  std::stringstream ss;
  ss << "Total numbers: " << keysInCache_.size() << std::endl;
  for (auto const &segmentKey: keysInCache_) {
    ss << fmt::format("Key: {};\tAvgValue: {}\tValue: {}Value2: {}\tHitNum: {}", segmentKey->toString(), segmentKey->getMetadata()->avgValue(), segmentKey->getMetadata()->value(), segmentKey->getMetadata()->value2(), segmentKey->getMetadata()->hitNum()) << std::endl;
  }
  ss << "Max size: " << maxSize_ << std::endl;
  ss << "Free size: " << freeSize_ << std::endl;
  return ss.str();
}

CachingPolicyId WFBRCachingPolicy::id() {
  return WFBR;
}
