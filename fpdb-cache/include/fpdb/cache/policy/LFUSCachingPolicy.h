//
// Created by ec2-user on 12/24/20.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_FBRSCACHINGPOLICY_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_FBRSCACHINGPOLICY_H

#include <fpdb/cache/policy/CachingPolicy.h>
#include <fpdb/cache/SegmentKey.h>
#include <memory>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <queue>

using namespace fpdb::cache;

namespace fpdb::cache::policy {

class LFUSCachingPolicy: public CachingPolicy {

public:
  explicit LFUSCachingPolicy(size_t maxSize,
                             std::shared_ptr<CatalogueEntry> catalogueEntry);
  LFUSCachingPolicy() = default;
  LFUSCachingPolicy(const LFUSCachingPolicy&) = default;
  LFUSCachingPolicy& operator=(const LFUSCachingPolicy&) = default;

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
  std::vector<std::shared_ptr<SegmentKey>> keysInCache_;
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keySetInCache_;
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keySet_;

  /**
   * tmp data structures for onToCache()  (OTC: onToCache)
   */
  size_t freeSizeOTC_{};
  std::vector<std::shared_ptr<SegmentKey>> keysInCacheOTC_;

  static bool lessValue(const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2);

  /**
   * For FBR, erasing only erases the element in keyInCache_, but not in keySet_ to keep history hitNum
   */
  void erase(const std::shared_ptr<SegmentKey> &key);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LFUSCachingPolicy& policy) {
    return f.object(policy).fields(f.field("type", policy.type_),
                                   f.field("maxSize", policy.maxSize_),
                                   f.field("freeSize", policy.freeSize_),
                                   f.field("segmentSizeMap", policy.segmentSizeMap_));
  }
};

}

#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_FBRSCACHINGPOLICY_H
