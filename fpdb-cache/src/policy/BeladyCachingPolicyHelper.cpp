//
// Created by Yifei Yang on 11/15/21.
//

#include <fpdb/cache/policy/BeladyCachingPolicyHelper.h>

namespace fpdb::cache::policy {

void BeladyCachingPolicyHelper::addToSegmentQueryNumMappings(int queryNum, const std::shared_ptr<SegmentKey>& segmentKey) {
  auto queryNumKeyEntry = queryNumToInvolvedSegments_.find(queryNum);
  if (queryNumKeyEntry != queryNumToInvolvedSegments_.end()) {
    queryNumKeyEntry->second.insert(segmentKey);
  } else {
    // first time seeing this queryNum, create a map entry for it
    std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> segmentKeys;
    segmentKeys.insert(segmentKey);
    queryNumToInvolvedSegments_.emplace(queryNum, segmentKeys);
  }

  auto segmentKeyEntry = segmentKeysToInvolvedQueryNums_.find(segmentKey);
  if (segmentKeyEntry != segmentKeysToInvolvedQueryNums_.end()) {
    segmentKeyEntry->second.insert(queryNum);
  } else {
    // first time seeing this key, create a map entry for it
    std::set<int> queryNumbers;
    queryNumbers.insert(queryNum);
    segmentKeysToInvolvedQueryNums_.emplace(segmentKey, queryNumbers);
  }
}

void BeladyCachingPolicyHelper::setSegmentSizeMap(
        const unordered_map<std::shared_ptr<cache::SegmentKey>, size_t, cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate> &segmentSizeMap) {
  segmentSizeMap_ = segmentSizeMap;
}

size_t BeladyCachingPolicyHelper::getSegmentSize(const shared_ptr<cache::SegmentKey> &segmentKey) const {
  auto key = segmentSizeMap_.find(segmentKey);
  if (key != segmentSizeMap_.end()) {
    return segmentSizeMap_.at(segmentKey);
  }
  throw std::runtime_error("Segment key not found in getSegmentSize: " + segmentKey->toString());
}

int BeladyCachingPolicyHelper::querySegmentNextUsedIn(const shared_ptr<cache::SegmentKey> &segmentKey, int currentQuery) {
  auto key = segmentKeysToInvolvedQueryNums_.find(segmentKey);
  if (key != segmentKeysToInvolvedQueryNums_.end()) {
    auto involvedQueriesList = key->second;
    auto nextQueryIt = involvedQueriesList.upper_bound(currentQuery);
    if (nextQueryIt != involvedQueriesList.end()) {
      return *nextQueryIt;
    }
    // TODO: segment never used again so return -1 to indicate this. Probably want a better strategy in the future
    return -1;
  }
  // must not exist in our queryNums, throw an error as we should have never called this then
  throw std::runtime_error("Error, " + segmentKey->toString() + " next query requested but never should have been used");
}

std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>
BeladyCachingPolicyHelper::getSegmentsInQuery(int queryNum) {
  auto key = queryNumToInvolvedSegments_.find(queryNum);
  if (key != queryNumToInvolvedSegments_.end()) {
    return key->second;
  }
  // we must not have populated this queryNum->[segments used] mapping for this queryNum,
  // throw an error as we should have never called this then
  throw std::runtime_error("Error, " + std::to_string(queryNum) + " not populated in segmentKeysToInvolvedQueryNums_ in MiniCatalogue.cpp");
}

int BeladyCachingPolicyHelper::getCurrentQueryNum() const {
  return currentQueryNum_;
}

void BeladyCachingPolicyHelper::setCurrentQueryNum(int currentQueryNum) {
  currentQueryNum_ = currentQueryNum;
}

}