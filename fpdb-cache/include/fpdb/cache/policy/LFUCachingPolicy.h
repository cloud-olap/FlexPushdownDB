//
// Created by Yifei Yang on 8/3/20.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_FBRCACHINGPOLICY_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_FBRCACHINGPOLICY_H

#include <fpdb/cache/policy/CachingPolicy.h>
#include <fpdb/cache/SegmentKey.h>
#include <memory>
#include <list>
#include <forward_list>
#include <unordered_set>
#include <unordered_map>
#include <vector>

using namespace fpdb::cache;

namespace fpdb::cache::policy {

class LFUCachingPolicy: public CachingPolicy {

public:
  explicit LFUCachingPolicy(size_t maxSize,
                            std::shared_ptr<CatalogueEntry> catalogueEntry);
  LFUCachingPolicy() = default;
  LFUCachingPolicy(const LFUCachingPolicy&) = default;
  LFUCachingPolicy& operator=(const LFUCachingPolicy&) = default;

  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) override;
  std::string showCurrentLayout() override;
  std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>> getKeysetInCachePolicy() override;
  std::string toString() override;
  void onNewQuery() override;
  void onClear() override;

private:
  std::unordered_map<int, std::list<std::shared_ptr<SegmentKey>>> freqMap_;
  std::unordered_map<std::shared_ptr<SegmentKey>, std::list<std::shared_ptr<SegmentKey>>::iterator, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keyMap_;
  int minFreq_;
  std::unordered_set<int> freqSet_;
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keySet_;

  /**
   * Store cache replacement decisions
   */
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keysToReplace_;
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> estimateCachingDecisions_;

  static bool lessValue (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2);

  [[maybe_unused]] void addEstimateCachingDecision(const std::shared_ptr<SegmentKey> &in, const std::shared_ptr<SegmentKey> &out);
  void removeEstimateCachingDecision(const std::shared_ptr<SegmentKey> &in);

  /**
   * For FBR, erasing only erases the element in keyInCache_, but not in keySet_ to keep history hitNum
   */
  void eraseFreqMap(int freq, std::list<std::shared_ptr<SegmentKey>>::iterator it);
  void insert(int freq, const std::shared_ptr<SegmentKey> &key);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LFUCachingPolicy& policy) {
    return f.object(policy).fields(f.field("type", policy.type_),
                                   f.field("maxSize", policy.maxSize_),
                                   f.field("freeSize", policy.freeSize_));
  }
};

}


#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_FBRCACHINGPOLICY_H
