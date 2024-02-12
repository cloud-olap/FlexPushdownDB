//
// Created by Matt Woicik on 9/22/20.
//

#include <fpdb/cache/policy/BeladyCachingPolicy.h>
#include <fmt/format.h>
#include <sstream>
#include <cstdlib>
#include <utility>

using namespace fpdb::cache::policy;

bool BeladyCachingPolicy::lessKeyValue (const std::shared_ptr<SegmentKey> &key1, 
                                        const std::shared_ptr<SegmentKey> &key2) {
  // Used to ensure weak ordering, can't have a key be less than itself
  int currentQuery = helper_.getCurrentQueryNum();
  // note that this is -1 if the key is never used again
  int key1NextUse = helper_.querySegmentNextUsedIn(key1, currentQuery);
  int key2NextUse = helper_.querySegmentNextUsedIn(key2, currentQuery);

  // For Belady key1 has lessValue than key2 if key2 is next used before key 1 is next used
  // If they are both next used at the same time, then key1 has lessValue if it is smaller
  if (key1NextUse == key2NextUse) {
    size_t key1Size = helper_.getSegmentSize(key1);
    size_t key2Size = helper_.getSegmentSize(key2);
    return key1Size < key2Size;
  } else if (key1NextUse == -1 || (key2NextUse != -1 && key2NextUse < key1NextUse)) {
    return true;
  } else if(key2NextUse == -1 || (key1NextUse != -1 && key1NextUse < key2NextUse)) {
    return false;
  }
  throw std::runtime_error("Error, lessKeyValue reached code that should not be reachable");
}

BeladyCachingPolicy::BeladyCachingPolicy(size_t maxSize,
                                         std::shared_ptr<CatalogueEntry> catalogueEntry) :
  CachingPolicy(BELADY,
                maxSize,
                std::move(catalogueEntry),
                true) {
  // set segment sizes to helper_
  helper_.setSegmentSizeMap(getSegmentSizeMap());
}

void BeladyCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &) {
  // Nothing to do for Belady caching policy
}

void BeladyCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  erase(key);
}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>
BeladyCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
  auto removableKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  auto segmentSize = key->getMetadata()->size();
  auto preComputedSize = getSegmentSize(key);
  // make sure segmentSize within 1% of our expected size, if not then something went wrong
  int absSizeDiff = abs((int) (segmentSize - preComputedSize));
  int onePercentSizeDiff = (int) ((float)segmentSize * 0.01);
  if (absSizeDiff > onePercentSizeDiff) {
    throw std::runtime_error("Error, segment has wrong size when compared to expected size");
  }
  if (maxSize_ < segmentSize) {
    return std::nullopt;
  }

  int currentQueryNum = helper_.getCurrentQueryNum();
  auto queryKey = queryNumToKeysInCache_.find(currentQueryNum);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, query " + std::to_string(currentQueryNum) + " not populated in queryNumToKeysInCache_ in BeladyCachingPolicy.cpp");
  }
  auto keysThatShouldBeCached = queryKey->second;

  // Shouldn't cache segments that aren't supposed to be in the cache at the end of this query
  if (keysThatShouldBeCached->find(key) == keysThatShouldBeCached->end()) {
    return std::nullopt;
  }

  // if room for this key no need to evict anything
  if (segmentSize < freeSize_) {
    keysInCache_.insert(key);
    freeSize_ -= segmentSize;
    return std::optional(removableKeys);
  }

  // remove all keys in the cache that shouldn't be present after this query
  // amortized operation as usually nothing done and we do earlier empty return
  for (const auto& potentialKeyToRemove: keysInCache_) {
    if (keysThatShouldBeCached->find(potentialKeyToRemove) == keysThatShouldBeCached->end()) {
      removableKeys->emplace_back(potentialKeyToRemove);
      freeSize_ += potentialKeyToRemove->getMetadata()->size();
    }
  }
  // remove these keys from the cache
  for (const auto& removableKey: *removableKeys) {
    keysInCache_.erase(removableKey);
  }

  // bring in
  keysInCache_.insert(key);
  freeSize_ -= segmentSize;

  // Make sure we never use more than our cache size
  if (freeSize_ < 0) {
    throw std::runtime_error("Error, freeSize_ < 0, is: " + std::to_string(freeSize_));
  }

  return std::optional(removableKeys);
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
BeladyCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  auto keysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  int currentQueryNum = helper_.getCurrentQueryNum();
  auto queryKey = queryNumToKeysInCache_.find(currentQueryNum);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, " + std::to_string(currentQueryNum) + " not populated in queryNumToKeysInCache_ in BeladyCachingPolicy.cpp");
  }

  // make sure that all keys are expected in this query
//  auto keysInCurrentQuery = getSegmentsInQuery(currentQueryNum);
//  for (auto key: *segmentKeys) {
//    if (keysInCurrentQuery->find(key) == keysInCurrentQuery->end()) {
//      throw std::runtime_error("Error, in query# " + std::to_string(currentQueryNum) + " got an unexpected segment\n " + key->toString());
//    }
//  }

  auto keysThatShouldBeCached = queryKey->second;
  // decide whether to cache
  for (const auto& key: *segmentKeys) {
    // see if we decided to cache this segment for this query number
    if (keysThatShouldBeCached->find(key) != keysThatShouldBeCached->end()) {
      keysToCache->emplace_back(key);
    }
  }
  return keysToCache;
}

void BeladyCachingPolicy::erase(const std::shared_ptr<SegmentKey> &key) {
  keysInCache_.erase(std::find(keysInCache_.begin(), keysInCache_.end(), key));
}

std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>
BeladyCachingPolicy::getKeysetInCachePolicy() {
  auto keysetInCachePolicy = std::make_shared<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
  keysetInCachePolicy->insert(keysInCache_.begin(), keysInCache_.end());
  return keysetInCachePolicy;
}

std::string BeladyCachingPolicy::showCurrentLayout() {
  std::stringstream ss;
  ss << "Total numbers: " << keysInCache_.size() << std::endl;
  for (auto const &segmentKey: keysInCache_) {
    int queryNum = helper_.getCurrentQueryNum();
    int keyNextUse = helper_.querySegmentNextUsedIn(segmentKey, queryNum);
    size_t keySize = getSegmentSize(segmentKey);
    ss << fmt::format("Key: {};\tSize: {}\n Next Use: {}", segmentKey->toString(), keySize, keyNextUse) << std::endl;
  }
  ss << "Max size: " << maxSize_ << std::endl;
  ss << "Free size: " << freeSize_ << std::endl;
  return ss.str();
}

[[maybe_unused]] std::string BeladyCachingPolicy::printHitsAndMissesPerQuery() {
  std::stringstream ss;
  ss << "Total Queries: " << numQueries_ << std::endl;
  int originalQueryNum = helper_.getCurrentQueryNum();
  for (int queryNum = 2; queryNum <= numQueries_; queryNum++) {
    ss << std::endl;
    helper_.setCurrentQueryNum(queryNum);
    auto keysInCurrentQuery = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    const auto &keysInCurrentQuerySet = helper_.getSegmentsInQuery(queryNum);
    keysInCurrentQuery->insert(keysInCurrentQuery->end(), keysInCurrentQuerySet.begin(), keysInCurrentQuerySet.end());
    std::sort(keysInCurrentQuery->begin(), keysInCurrentQuery->end(),
              [this](const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
                return lessKeyValue(key1, key2);
              });
    // Reverse this ordering as we want to list keys with the greatest value first
    std::reverse(keysInCurrentQuery->begin(), keysInCurrentQuery->end());

    int previousQuery = queryNum - 1;
    auto queryKey = queryNumToKeysInCache_.find(previousQuery);
    if (queryKey == queryNumToKeysInCache_.end()) {
      throw std::runtime_error("Error, " + std::to_string(previousQuery) + " not populated in queryNumToKeysInCache_ in BeladyCachingPolicy.cpp");
    }
    auto keysThatAreInCacheBeforeCurrentQuery = queryKey->second;
    int numHits = 0;
    int numMisses = 0;

    ss << "*****Hits in Query #" << queryNum << std::endl;
    for (auto const &segmentKey: *keysInCurrentQuery) {
      if (keysThatAreInCacheBeforeCurrentQuery->find(segmentKey) != keysThatAreInCacheBeforeCurrentQuery->end()) {
        numHits++;
        int keyNextUse = helper_.querySegmentNextUsedIn(segmentKey, queryNum);
        size_t keySize = getSegmentSize(segmentKey);
        ss << fmt::format("Next Use: {}\tSize: {}\tKey: {};", keyNextUse, keySize, segmentKey->toString()) << std::endl;
      }
    }
    ss << "*****Misses in Query #" << queryNum << std::endl;
    for (auto const &segmentKey: *keysInCurrentQuery) {
      if (keysThatAreInCacheBeforeCurrentQuery->find(segmentKey) == keysThatAreInCacheBeforeCurrentQuery->end()) {
        numMisses++;
        int keyNextUse = helper_.querySegmentNextUsedIn(segmentKey, queryNum);
        size_t keySize = getSegmentSize(segmentKey);
        ss << fmt::format("Next Use: {}\tSize: {}\tKey: {};", keyNextUse, keySize, segmentKey->toString()) << std::endl;
      }
    }
    ss << "Total hits/misses: " << numHits << "/" << numMisses << std::endl;
  }
  helper_.setCurrentQueryNum(originalQueryNum);
  return ss.str();
}

std::string BeladyCachingPolicy::printLayoutAfterEveryQuery() {
  std::stringstream ss;
  ss << "Total Queries: " << numQueries_ << std::endl;
  int originalQueryNum = helper_.getCurrentQueryNum();
  for (int queryNum = 1; queryNum <= numQueries_; queryNum++) {
    helper_.setCurrentQueryNum(queryNum);
    ss << "*****Segments in Query #" << queryNum << std::endl;
    auto keysInCurrentQuery = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    const auto &keysInCurrentQuerySet = helper_.getSegmentsInQuery(queryNum);
    keysInCurrentQuery->insert(keysInCurrentQuery->end(), keysInCurrentQuerySet.begin(), keysInCurrentQuerySet.end());
    std::sort(keysInCurrentQuery->begin(), keysInCurrentQuery->end(),
              [this](const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
                return lessKeyValue(key1, key2);
              });
    // Reverse this ordering as we want keys with the greatest value first
    std::reverse(keysInCurrentQuery->begin(), keysInCurrentQuery->end());
    for (auto const &segmentKey: *keysInCurrentQuery) {
      int keyNextUse = helper_.querySegmentNextUsedIn(segmentKey, queryNum);
      size_t keySize = getSegmentSize(segmentKey);
      ss << fmt::format("Next Use: {}\tSize: {}\tKey: {};", keyNextUse, keySize, segmentKey->toString()) << std::endl;
    }
    ss << "*****After query #" << queryNum << " cache contains:"<< std::endl;
    auto keysInCacheAfterQueryNum = queryNumToKeysInCache_.at(queryNum);
    size_t freeSize = maxSize_;
    for (auto const &segmentKey: *keysInCacheAfterQueryNum) {
      int keyNextUse = helper_.querySegmentNextUsedIn(segmentKey, queryNum);
      size_t keySize = getSegmentSize(segmentKey);
      ss << fmt::format("Next Use: {}\tSize: {}\tKey: {};", keyNextUse, keySize, segmentKey->toString()) << std::endl;
      freeSize -= keySize;
    }
    ss << "Max size: " << maxSize_ << std::endl;
    ss << "Free size: " << freeSize << std::endl;
    float_t percentCacheUsed = 1.0 - ((float_t) freeSize / (float_t) maxSize_);
    ss << "Percent cache used: " << percentCacheUsed << std::endl;
  }
  helper_.setCurrentQueryNum(originalQueryNum);
  return ss.str();
}

void BeladyCachingPolicy::assertDecreasingOrderingOfSegmentKeys(const std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>& segmentKeys) {
  for (size_t i = 0; i < segmentKeys->size() - 1; i++) {
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
    helper_.setCurrentQueryNum(queryNum);

    // Create list of potential keys to have in our cache after this query from keys already in the cache
    // and keys that are about to be queried
    auto keysInCurrentQuery = helper_.getSegmentsInQuery(queryNum);
    // add keys in Current Query that aren't already in the cache
    auto allPotentialKeysToCache = std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>();
    allPotentialKeysToCache.insert(keysInCache->begin(), keysInCache->end());
    allPotentialKeysToCache.insert(keysInCurrentQuery.begin(), keysInCurrentQuery.end());

    auto potentialKeysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    potentialKeysToCache->insert(potentialKeysToCache->end(), allPotentialKeysToCache.begin(), allPotentialKeysToCache.end());

    // reset our keysInCache to be empty for this query and populate it
    keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    std::sort(potentialKeysToCache->begin(), potentialKeysToCache->end(),
              [this](const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
                return lessKeyValue(key1, key2);
              });
    // Reverse this ordering as we want keys with the greatest value first
    std::reverse(potentialKeysToCache->begin(), potentialKeysToCache->end());
//    assertDecreasingOrderingOfSegmentKeys(potentialKeysToCache);


    // Multiply by 0.99 as pre computed segment sizes are sometimes slightly off (< 1%), so this buffer
    // ensures we never store more in our cache than the max cache size
    size_t remainingCacheSize = maxSize_ * 0.99;
    for (const auto& segmentKey : *potentialKeysToCache) {
      size_t segmentSize = getSegmentSize(segmentKey);
      int keyNextUse = helper_.querySegmentNextUsedIn(segmentKey, queryNum);
      // Decide to cache key if there is room and the key is used again ie: keyNextUse != -1
      if (segmentSize < remainingCacheSize && keyNextUse != -1) {
        keysInCache->emplace_back(segmentKey);
        remainingCacheSize -= segmentSize;
      }
    }
    auto keysToCacheSet = std::make_shared<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
    keysToCacheSet->insert(keysInCache->begin(), keysInCache->end());
    queryNumToKeysInCache_.emplace(queryNum, keysToCacheSet);
  }
}

std::string BeladyCachingPolicy::approximateExecutionHitRate(int warmBatchSize, int executeBatchSize) {
  std::stringstream ss;
  if (warmBatchSize + executeBatchSize > numQueries_) {
    throw std::runtime_error("Error, warmBatchSize + executeBatchSize: " + std::to_string(warmBatchSize + executeBatchSize) + ", is: less than numQueries_: " + std::to_string(numQueries_));
  }

  int totalHitNum = 0;
  int totalMissNum = 0;
  for (int queryNum = warmBatchSize + 1; queryNum <= numQueries_; ++queryNum) {

    auto keysInCurrentQuery = helper_.getSegmentsInQuery(queryNum);
    // can't have any cache hits on first query so handle separately
    if (queryNum == 1) {
      ss << "Query " << queryNum << " hit rate: " << 0.0 << " (hit, miss) = " << 0 << "," << keysInCurrentQuery.size() << std::endl;
      continue;
    }
    auto queryKey = queryNumToKeysInCache_.find(queryNum - 1);
    if (queryKey == queryNumToKeysInCache_.end()) {
      throw std::runtime_error("Error, " + std::to_string(queryNum - 1) + " not populated in queryNumToKeysInCache_ in BeladyCachingPolicy.cpp");
    }
    auto keysCached = queryKey->second;

    int currentHitNum = 0;
    int currentMissNum = 0;
    for (const auto& segmentKey : keysInCurrentQuery) {
      if (keysCached->find(segmentKey) != keysCached->end()) {
        currentHitNum += 1;
      } else {
        currentMissNum += 1;
      }
    }
    double queryHitRate = (double)currentHitNum / (double) (currentHitNum + currentMissNum);
    ss << "Query " << queryNum << " hit rate: " << queryHitRate << " (hit, miss) = " << currentHitNum << "," << currentMissNum << std::endl;
    totalHitNum += currentHitNum;
    totalMissNum += currentMissNum;
  }
  double totalHitRate = (double)totalHitNum / (double) (totalHitNum + totalMissNum);
  ss << std::endl << "Total hit rate: " << totalHitRate << " (hit, miss) = " << totalHitNum << "," << totalMissNum << std::endl;
  return ss.str();
}

[[maybe_unused]] void BeladyCachingPolicy::compareExpectedCachedKeysToActual(int queryNumberJustFinished) {
  auto queryKey = queryNumToKeysInCache_.find(queryNumberJustFinished);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, " + std::to_string(queryNumberJustFinished) + " not populated in queryNumToKeysInCache_ in BeladyCachingPolicy.cpp");
  }
  auto keysThatShouldBeCached = queryKey->second;

  bool actualKeysDontMatch = false;
  for (const auto& segmentKey : *keysThatShouldBeCached) {
    if (keysInCache_.find(segmentKey) == keysInCache_.end()) {
      printf("Query %d Expected key: %s\n", queryNumberJustFinished, segmentKey->toString().c_str());
      actualKeysDontMatch = true;
    }
  }
  if (actualKeysDontMatch) {
    throw std::runtime_error("Actual keys in cache missing expected keys in cache");
  }
}

std::string BeladyCachingPolicy::toString() {
  return "Belady";
}

void BeladyCachingPolicy::onNewQuery() {

}

void BeladyCachingPolicy::onClear() {
  keysInCache_.clear();
  queryNumToKeysInCache_.clear();
  numQueries_ = 0;
}
