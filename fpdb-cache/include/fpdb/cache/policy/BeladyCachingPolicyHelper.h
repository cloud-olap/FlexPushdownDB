//
// Created by Yifei Yang on 11/15/21.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_POLICY_BELADYCACHINGPOLICYHELPER_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_POLICY_BELADYCACHINGPOLICYHELPER_H

#include <fpdb/cache/SegmentKey.h>
#include <unordered_set>
#include <unordered_map>
#include <set>

using namespace fpdb::cache;
using namespace std;

namespace fpdb::cache::policy {

class BeladyCachingPolicyHelper {
public:
  /**
  * functions and temp vars to generate Belady decisions
  */
  // Used to populate the queryNumToInvolvedSegments_ and segmentKeysToInvolvedQueryNums_ mappings
  void addToSegmentQueryNumMappings(int queryNum, const std::shared_ptr<SegmentKey>& segmentKey);

  void setSegmentSizeMap(const unordered_map<std::shared_ptr<cache::SegmentKey>, size_t,
                         cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate> &segmentSizeMap);

  size_t getSegmentSize(const std::shared_ptr<cache::SegmentKey>& segmentKey) const;

  // Return the next query that this segmentKey is used in, if not used again according to the
  // segmentKeysToInvolvedQueryNums_ mapping set via setSegmentKeysToInvolvedQueryNums then return -1
  // Throws runtime exception if segmentKeysToInvolvedQueryNums_ is never set via setSegmentKeysToInvolvedQueryNums
  int querySegmentNextUsedIn(const shared_ptr<SegmentKey>& segmentKey, int currentQuery);
  
  // Throws runtime exception if queryNum not present, which ensures failing fast in case of queryNum not being present
  // If use cases of this method expand we can change this if necessary.
  unordered_set<shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>
  getSegmentsInQuery(int queryNum);

  int getCurrentQueryNum() const;
  void setCurrentQueryNum(int currentQueryNum);

private:
  std::unordered_map<std::shared_ptr<cache::SegmentKey>, size_t,
          cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate> segmentSizeMap_;
  unordered_map<int, unordered_set<shared_ptr<cache::SegmentKey>,
          cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>> queryNumToInvolvedSegments_;
  unordered_map<shared_ptr<cache::SegmentKey>, set<int>,
          cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate> segmentKeysToInvolvedQueryNums_;
  int currentQueryNum_;
};

}


#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_POLICY_BELADYCACHINGPOLICYHELPER_H
