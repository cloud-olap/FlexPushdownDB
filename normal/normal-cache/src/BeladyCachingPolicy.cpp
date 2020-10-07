//
// Created by Matt Woicik on 9/22/20.
//

#include <sstream>
#include <fmt/format.h>
#include <cstdlib>
#include "normal/cache/BeladyCachingPolicy.h"
#include "normal/connector/MiniCatalogue.h"

std::shared_ptr<normal::connector::MiniCatalogue> normal::cache::beladyMiniCatalogue;

using namespace normal::cache;

bool lessKeyValue (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  // Used to ensure weak ordering, can't have a key be less than itself
  int currentQuery = beladyMiniCatalogue->getCurrentQueryNum();
  // note that this is -1 if the key is never used again
  int key1NextUse = beladyMiniCatalogue->querySegmentNextUsedIn(key1, currentQuery);
  int key2NextUse = beladyMiniCatalogue->querySegmentNextUsedIn(key2, currentQuery);

  // For Belady key1 has lessValue than key2 if key2 is next used before key 1 is next used
  // If they are both next used at the same time, then key1 has lessValue if it is smaller
  if (key1NextUse == key2NextUse) {
    size_t key1Size = beladyMiniCatalogue->getSegmentSize(key1);
    size_t key2Size = beladyMiniCatalogue->getSegmentSize(key2);
    return key1Size < key2Size;
  } else if (key1NextUse == -1 || (key2NextUse != -1 && key2NextUse < key1NextUse)) {
    return true;
  } else if(key2NextUse == -1 || (key1NextUse != -1 && key1NextUse < key2NextUse)) {
    return false;
  }
  throw std::runtime_error("Error, lessKeyValue reached code that should not be reachable");
}

BeladyCachingPolicy::BeladyCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) :
        CachingPolicy(maxSize, mode) {}

std::shared_ptr<BeladyCachingPolicy> BeladyCachingPolicy::make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) {
  return std::make_shared<BeladyCachingPolicy>(maxSize, mode);
}

void BeladyCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &key) {
  // Nothing to do for Belady caching policy
}

void BeladyCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  erase(key);
}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>
BeladyCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
  auto removableKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  auto segmentSize = key->getMetadata()->size();
  auto preComputedSize = beladyMiniCatalogue->getSegmentSize(key);
  // make sure segmentSize within 1% of our expected size, if not then something went wrong
  int absSizeDiff = abs((int) (segmentSize - preComputedSize));
  int onePercentSizeDiff = (int) ((float)segmentSize * 0.01);
  if (absSizeDiff > onePercentSizeDiff) {
    throw std::runtime_error("Error, segment has wrong size when compared to expected size");
  }
  if (maxSize_ < segmentSize) {
    return std::nullopt;
  }

  int currentQueryNum = beladyMiniCatalogue->getCurrentQueryNum();
  auto queryKey = queryNumToKeysInCache_.find(currentQueryNum);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, query " + std::to_string(currentQueryNum) + " not populated in queryNumToKeysInCache_ in BeladyCachingPolicy.cpp");
  }
  auto keysThatShouldBeCached = queryKey->second;

  // remove keys in the cache that shouldn't be present after this query to make adequate room
  for (auto potentialKeyToRemove: keysInCache_) {
    bool evictKey = true;
    for (auto keyThatShouldBeCached: *keysThatShouldBeCached) {
      if (*potentialKeyToRemove == *keyThatShouldBeCached) {
        evictKey = false;
        break;
      }
    }
    if (evictKey) {
      removableKeys->emplace_back(potentialKeyToRemove);
      freeSize_ += potentialKeyToRemove->getMetadata()->size();
      // this would mean we have freed up enough room for this new key so we can stop evicting keys
      if (freeSize_ > segmentSize) {
        break;
      }
    }
  }

  // bring in
  keysInCache_.emplace_back(key);
  freeSize_ -= segmentSize;

  // Make sure we never use more than our cache size
  if (freeSize_ < 0) {
    throw std::runtime_error("Error, freeSize_ < 0, is: " + std::to_string(freeSize_));
  }

  return std::optional(removableKeys);
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
BeladyCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  if (mode_->id() == normal::plan::operator_::mode::ModeId::PullupCaching) {
    return segmentKeys;
  }

  auto keysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  int currentQueryNum = beladyMiniCatalogue->getCurrentQueryNum();
  auto queryKey = queryNumToKeysInCache_.find(currentQueryNum);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, " + std::to_string(currentQueryNum) + " not populated in queryNumToKeysInCache_ in BeladyCachingPolicy.cpp");
  }
  auto keysThatShouldBeCached = queryKey->second;

  // decide whether to cache
  for (auto key: *segmentKeys) {
    // see if we decided to cache this segment for this query number
    for (auto keyToCache: *keysThatShouldBeCached) {
      if (*key == *keyToCache) {
        keysToCache->emplace_back(key);
      }
    }
  }
  return keysToCache;
}

void BeladyCachingPolicy::erase(const std::shared_ptr<SegmentKey> &key) {
  keysInCache_.erase(std::find(keysInCache_.begin(), keysInCache_.end(), key));
}

std::string BeladyCachingPolicy::showCurrentLayout() {
  std::stringstream ss;
  ss << "Total numbers: " << keysInCache_.size() << std::endl;
  for (auto const &segmentKey: keysInCache_) {
    int queryNum = beladyMiniCatalogue->getCurrentQueryNum();
    int keyNextUse = beladyMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
    size_t keySize = beladyMiniCatalogue->getSegmentSize(segmentKey);
    ss << fmt::format("Key: {};\tSize: {}\n Next Use: {}", segmentKey->toString(), keySize, keyNextUse) << std::endl;
  }
  ss << "Max size: " << maxSize_ << std::endl;
  ss << "Free size: " << freeSize_ << std::endl;
  return ss.str();
}

std::string BeladyCachingPolicy::printLayoutAfterEveryQuery() {
  std::stringstream ss;
  ss << "Total Queries: " << numQueries_ << std::endl;
  int originalQueryNum = beladyMiniCatalogue->getCurrentQueryNum();
  for (int queryNum = 1; queryNum <= numQueries_; queryNum++) {
    beladyMiniCatalogue->setCurrentQueryNum(queryNum);
    ss << "*****Segments in Query #" << queryNum << std::endl;
    auto keysInCurrentQuery = beladyMiniCatalogue->getSegmentsInQuery(queryNum);
    std::sort(keysInCurrentQuery->begin(), keysInCurrentQuery->end(), lessKeyValue);
    // Reverse this ordering as we want keys with the greatest value first
    std::reverse(keysInCurrentQuery->begin(), keysInCurrentQuery->end());
    for (auto const &segmentKey: *keysInCurrentQuery) {
      int keyNextUse = beladyMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
      size_t keySize = beladyMiniCatalogue->getSegmentSize(segmentKey);
      ss << fmt::format("Next Use: {}\tSize: {}\tKey: {};", keyNextUse, keySize, segmentKey->toString()) << std::endl;
    }
    ss << "*****After query #" << queryNum << " cache contains:"<< std::endl;
    auto keysInCacheAfterQueryNum = queryNumToKeysInCache_.at(queryNum);
    size_t freeSize = maxSize_;
    for (auto const &segmentKey: *keysInCacheAfterQueryNum) {
      int keyNextUse = beladyMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
      size_t keySize = beladyMiniCatalogue->getSegmentSize(segmentKey);
      ss << fmt::format("Next Use: {}\tSize: {}\tKey: {};", keyNextUse, keySize, segmentKey->toString()) << std::endl;
      freeSize -= keySize;
    }
    ss << "Max size: " << maxSize_ << std::endl;
    ss << "Free size: " << freeSize << std::endl;
    float_t percentCacheUsed = 1.0 - ((float_t) freeSize / (float_t) maxSize_);
    ss << "Percent cache used: " << percentCacheUsed << std::endl;
  }
  beladyMiniCatalogue->setCurrentQueryNum(originalQueryNum);
  return ss.str();
}

void assertNoDuplicateSegmentKeys(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  for (int i = 0; i < segmentKeys->size(); i++) {
    for (int j = i + 1; j < segmentKeys->size(); j++) {
      auto key1 = segmentKeys->at(i);
      auto key2 = segmentKeys->at(j);
      if (*key1 == *key2) {
        throw std::runtime_error("Error, identical keys present when generating caching decisions");
      }
    }
  }
}

void assertDecreasingOrderingOfSegmentKeys(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  for (int i = 0; i < segmentKeys->size() - 1; i++) {
    auto key1 = segmentKeys->at(i);
    auto key2 = segmentKeys->at(i + 1);
    if (lessKeyValue(key1, key2)) {
      lessKeyValue(key1, key2);
      throw std::runtime_error("Error, identical keys present when generating caching decisions");
    }
  }
}

void BeladyCachingPolicy::generateCacheDecisions(int numQueries) {
  numQueries_ = numQueries;
  auto keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  for (int queryNum = 1; queryNum <= numQueries; ++queryNum) {
    beladyMiniCatalogue->setCurrentQueryNum(queryNum);

    // Create list of potential keys to have in our cache after this query from keys already in the cache
    // and keys that are about to be queried
    auto potentialKeysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    potentialKeysToCache->insert(potentialKeysToCache->end(), keysInCache->begin(), keysInCache->end());
    auto keysInCurrentQuery = beladyMiniCatalogue->getSegmentsInQuery(queryNum);
    // add keys in Current Query that aren't already in the cache
    for (auto key: *keysInCurrentQuery) {
      bool keyNotInPotentialKeys = true;
      for (auto keyAlreadyAccountedFor: *potentialKeysToCache) {
        if (*key == *keyAlreadyAccountedFor) {
          keyNotInPotentialKeys = false;
          break;
        }
      }
      if (keyNotInPotentialKeys) {
        potentialKeysToCache->emplace_back(key);
      }
    }
    assertNoDuplicateSegmentKeys(potentialKeysToCache);

    // reset our keysInCache to be empty for this query and populate it
    keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    std::sort(potentialKeysToCache->begin(), potentialKeysToCache->end(), lessKeyValue);
    // Reverse this ordering as we want keys with the greatest value first
    std::reverse(potentialKeysToCache->begin(), potentialKeysToCache->end());
    assertDecreasingOrderingOfSegmentKeys(potentialKeysToCache);


    // Multiply by 0.99 as pre computed segment sizes are sometimes slightly off (< 1%), so this buffer
    // ensures we never store more in our cache than the max cache size
    size_t remainingCacheSize = maxSize_ * 0.99;
    for (auto segmentKey : *potentialKeysToCache) {
      size_t segmentSize = beladyMiniCatalogue->getSegmentSize(segmentKey);
      int keyNextUse = beladyMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
      // Decide to cache key if there is room and the key is used again ie: keyNextUse != -1
      if (segmentSize < remainingCacheSize && keyNextUse != -1) {
        keysInCache->emplace_back(segmentKey);
        remainingCacheSize -= segmentSize;
      }
    }
    queryNumToKeysInCache_.emplace(queryNum, keysInCache);
  }
}

// remove the plan to cache any segments that we will evict before using again
void BeladyCachingPolicy::removeUnnecessaryPullUps() {
  // iterate through queries 1 to max number of queries (queryNum_)
  // and remove segments that won't be used in the future
  for (int i = 1; i <= numQueries_; i++) {
    auto keysToEvictForQueryi = std::vector<std::shared_ptr<SegmentKey>>();
    auto keysInCacheAfterQueryi = queryNumToKeysInCache_.at(i);
    for (auto key: *keysInCacheAfterQueryi) {
      int keyNextUse = beladyMiniCatalogue->querySegmentNextUsedIn(key, i);
      // Shouldn't be caching keys we never use again based on logic in generateCacheDecisions,
      // so throw an error if it occurs
      if (keyNextUse == -1) {
        throw std::runtime_error("Error, decision to cache segment that is never used again");
      }
      bool keyPresentUntilUsedAgain = true;
      for (int j = i + 1; j < keyNextUse; j++) {
        auto keysInCacheAfterQueryj = queryNumToKeysInCache_.at(j);
        bool keyEvictedDuringQueryj = true;
        for (auto cachedKey: *keysInCacheAfterQueryj) {
          if (*key == *cachedKey) {
            keyEvictedDuringQueryj = false;
            break;
          }
        }
        if (keyEvictedDuringQueryj) {
          keyPresentUntilUsedAgain = false;
          break;
        }
      }

      if (!keyPresentUntilUsedAgain) {
        keysToEvictForQueryi.emplace_back(key);
      }
    }

    for (auto keyToEvict: keysToEvictForQueryi) {
      // if the key isn't present until it is used again, remove all decisions to have it in the cache
      // until the next query, as it costs more to pull it up than to pushdown the predicates
      // for a one time access
      int keyNextUse = beladyMiniCatalogue->querySegmentNextUsedIn(keyToEvict, i);
      for (int j = i; j < keyNextUse; j++) {
        auto keysInCacheAfterQueryj = queryNumToKeysInCache_.at(j);
        for (int index = 0; index < keysInCacheAfterQueryj->size(); index++) {
          auto cachedKey = keysInCacheAfterQueryj->at(index);
          if (*keyToEvict == *cachedKey) {
            // we found the index to evict, done with this iteration
            keysInCacheAfterQueryj->erase(keysInCacheAfterQueryj->begin() + index);
            break;
          }
        }
      }
    }
  }
}

CachingPolicyId BeladyCachingPolicy::id() {
  return BELADY;
}
