//
// Created by matt on 2/6/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LRUCACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LRUCACHINGPOLICY_H

#include <memory>
#include <vector>
#include <list>
#include <forward_list>
#include <unordered_map>
#include <unordered_set>

#include "SegmentKey.h"
#include "CachingPolicy.h"

namespace normal::cache {

class LRUCachingPolicy: public CachingPolicy {

public:
  explicit LRUCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);
  static std::shared_ptr<LRUCachingPolicy> make();
  static std::shared_ptr<LRUCachingPolicy> make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);

  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) override;
  std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>> getKeysetInCachePolicy() override;
  std::string showCurrentLayout() override;
  CachingPolicyId id() override;
  std::string toString() override;
  void onNewQuery() override;

private:
  std::list<std::shared_ptr<SegmentKey>> usageQueue_;
  std::unordered_map<std::shared_ptr<SegmentKey>, std::list<std::shared_ptr<SegmentKey>>::iterator, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keyIndexMap_;

  void eraseLRU();
  void erase(const std::shared_ptr<SegmentKey> &key);
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LRUCACHINGPOLICY_H
