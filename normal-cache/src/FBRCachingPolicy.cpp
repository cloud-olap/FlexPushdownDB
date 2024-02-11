//
// Created by Yifei Yang on 8/3/20.
//

#include <sstream>
#include <fmt/format.h>
#include "normal/cache/FBRCachingPolicy.h"
#include <algorithm>
#include <utility>

using namespace normal::cache;

bool FBRCachingPolicy::lessValue (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  return (key1->getMetadata()->hitNum())
         < (key2->getMetadata()->hitNum());
}

FBRCachingPolicy::FBRCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) :
        CachingPolicy(maxSize, std::move(mode)), minFreq_(0) {}

std::shared_ptr<FBRCachingPolicy> FBRCachingPolicy::make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) {
  return std::make_shared<FBRCachingPolicy>(maxSize, mode);
}

void FBRCachingPolicy::eraseFreqMap(int freq, std::list<std::shared_ptr<SegmentKey>>::iterator it) {
  freqMap_[freq].erase(it);
  if (freqMap_[freq].empty()) {
    freqMap_.erase(freq);
    freqSet_.erase(freq);
    if (minFreq_ == freq) {
      // update minFreq_ to next min
      minFreq_ = *freqSet_.begin();
      for (auto freq1: freqSet_) {
        if (freq1 < minFreq_) {
          minFreq_ = freq1;
        }
      }
    }
  }
}

void FBRCachingPolicy::insert(int freq, const std::shared_ptr<SegmentKey> &key) {
  auto freqMapIt = freqMap_.find(freq);
  if (freqMapIt == freqMap_.end()) {
    std::list<std::shared_ptr<SegmentKey>> l;
    freqMap_.emplace(freq, l);
  }
  freqMap_[freq].emplace_back(key);
  auto listIt = freqMap_[freq].end();
  listIt--;
  keyMap_[key] = listIt;
  freqSet_.emplace(freq);
  if (minFreq_ == 0) {
    minFreq_ = freq;
  }
}

void FBRCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &key) {
  // update keySet_
  auto keyEntry = keySet_.find(key);
  std::shared_ptr<SegmentKey> realKey;
  if (keyEntry != keySet_.end()) {
    realKey = *keyEntry;
    realKey->getMetadata()->incHitNum();
  } else {
    realKey = key;
    keySet_.emplace(key);
  }

  // update keys in cache
  auto keyMapIt = keyMap_.find(realKey);
  if (keyMapIt != keyMap_.end()) {
    auto oldListIt = keyMapIt->second;
    auto a = *oldListIt;
    int freq = realKey->getMetadata()->hitNum();
    eraseFreqMap( freq - 1, oldListIt);
    insert(freq, realKey);
  }
}

void FBRCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  auto keyMapIt = keyMap_.find(key);
  if (keyMapIt != keyMap_.end()) {
    auto listIt = keyMapIt->second;
    eraseFreqMap(key->getMetadata()->hitNum(), listIt);
    keyMap_.erase(key);
  }
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

  // try to find space
  size_t lowSegmentSize = 0;
  int tmpMinFreq = minFreq_;
  auto tmpFreqSet = std::unordered_set<int>(freqSet_);
  enum Result {SUFFICIENT, INSUFFICIENT, UNKNOWN};
  Result res = UNKNOWN;
  if (freeSize_ >= segmentSize) {
    res = SUFFICIENT;
  } else {
    while (!tmpFreqSet.empty()) {
      for (auto const &candidateKey: freqMap_[tmpMinFreq]) {
        if (lessValue(candidateKey, realKey)) {
          removableKeys->emplace_back(candidateKey);
          lowSegmentSize += candidateKey->getMetadata()->size();
          if (lowSegmentSize + freeSize_ >= segmentSize) {
            res = SUFFICIENT;
            break;
          }
        } else {
          res = INSUFFICIENT;
          break;
        }
      }
      if (res == SUFFICIENT || res == INSUFFICIENT) {
        break;
      }
      tmpFreqSet.erase(tmpMinFreq);
      if (tmpFreqSet.size() > 0) {
        tmpMinFreq = *tmpFreqSet.begin();
        for (auto tmpFreq: tmpFreqSet) {
          if (tmpFreq < tmpMinFreq) {
            tmpMinFreq = tmpFreq;
          }
        }
      }
    }
  }

  removeEstimateCachingDecision(realKey);

  if (res == SUFFICIENT) {
    for (auto const &removableKey: *removableKeys) {
      auto listIt = keyMap_[removableKey];
      eraseFreqMap(removableKey->getMetadata()->hitNum(), listIt);
      keyMap_.erase(removableKey);
    }
    insert(realKey->getMetadata()->hitNum(), realKey);
    freeSize_ = freeSize_ + lowSegmentSize - segmentSize;
    return std::optional(removableKeys);
  } else {
    return std::nullopt;
  }
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
FBRCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
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
  for (const auto &key: *segmentKeys) {
    std::shared_ptr<SegmentKey> realKey;
    auto keyEntry = keySet_.find(key);
    // keys have been added to keySet_ in onLoad() before
    if (keyEntry != keySet_.end()) {
      realKey = *keyEntry;
    } else {
      throw std::runtime_error("onToCache: Key should exist in keySet_");
    }

    // it's expensive to find one lower-value unused key in cache, so if its value > min then cache it
    if (realKey->getMetadata()->hitNum() > minFreq_) {
      keysToCache->emplace_back(realKey);
    }
  }

  return keysToCache;
}

[[maybe_unused]] void FBRCachingPolicy::addEstimateCachingDecision(const std::shared_ptr<SegmentKey> &in,
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

std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>
FBRCachingPolicy::getKeysetInCachePolicy() {
  auto keysetInCachePolicy = std::make_shared<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
  for (auto const &keyMapIt: keyMap_) {
    auto segmentKey = keyMapIt.first;
    keysetInCachePolicy->insert(segmentKey);
  }
  return keysetInCachePolicy;
}

std::string FBRCachingPolicy::showCurrentLayout() {
  std::stringstream ss;
  ss << "Total numbers: " << keyMap_.size() << std::endl;
  for (auto const &keyMapIt: keyMap_) {
    auto segmentKey = keyMapIt.first;
    ss << fmt::format("Key: {};\tHitnum: {}\tSize: {}", segmentKey->toString(), segmentKey->getMetadata()->hitNum(), segmentKey->getMetadata()->size()) << std::endl;
  }
  ss << "Max size: " << maxSize_ << std::endl;
  ss << "Free size: " << freeSize_ << std::endl;
  return ss.str();
}

CachingPolicyId FBRCachingPolicy::id() {
  return FBR;
}

std::string FBRCachingPolicy::toString() {
  return "LFU";
}

void FBRCachingPolicy::onNewQuery() {

}
