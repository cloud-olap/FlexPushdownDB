//
// Created by matt on 2/6/20.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_LRUCACHINGPOLICY_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_LRUCACHINGPOLICY_H

#include <fpdb/cache/policy/CachingPolicy.h>
#include <fpdb/cache/SegmentKey.h>
#include <memory>
#include <vector>
#include <list>
#include <forward_list>
#include <unordered_map>
#include <unordered_set>

using namespace fpdb::cache;

namespace fpdb::cache::policy {

class LRUCachingPolicy: public CachingPolicy {

public:
  explicit LRUCachingPolicy(size_t maxSize,
                            std::shared_ptr<CatalogueEntry> catalogueEntry);
  LRUCachingPolicy() = default;
  LRUCachingPolicy(const LRUCachingPolicy&) = default;
  LRUCachingPolicy& operator=(const LRUCachingPolicy&) = default;

  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) override;
  std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>> getKeysetInCachePolicy() override;
  std::string showCurrentLayout() override;
  std::string toString() override;
  void onNewQuery() override;
  void onClear() override;

private:
  std::list<std::shared_ptr<SegmentKey>> usageQueue_;
  std::unordered_map<std::shared_ptr<SegmentKey>, std::list<std::shared_ptr<SegmentKey>>::iterator, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keyIndexMap_;

  void eraseLRU();
  void erase(const std::shared_ptr<SegmentKey> &key);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LRUCachingPolicy& policy) {
    return f.object(policy).fields(f.field("type", policy.type_),
                                   f.field("maxSize", policy.maxSize_),
                                   f.field("freeSize", policy.freeSize_));
  }
};

}

#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_LRUCACHINGPOLICY_H
