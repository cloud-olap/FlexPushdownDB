//
// Created by matt on 2/6/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LRUCACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LRUCACHINGPOLICY_H

#include <memory>
#include <list>
#include <forward_list>
#include <unordered_map>

#include "SegmentKey.h"
#include "CachingPolicy.h"

namespace normal::cache {

class LRUCachingPolicy: public CachingPolicy {

public:
  explicit LRUCachingPolicy(size_t maxSize);
  static std::shared_ptr<LRUCachingPolicy> make();
  static std::shared_ptr<LRUCachingPolicy> make(size_t maxSize);

  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  void onLoad(const std::shared_ptr<SegmentKey> &key) override;

private:
  size_t maxSize_;
  size_t freeSize_;
  std::list<std::shared_ptr<SegmentKey>> usageQueue_;
  std::unordered_map<std::shared_ptr<SegmentKey>, std::list<std::shared_ptr<SegmentKey>>::iterator, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keyIndexMap_;

  void eraseLRU();
  void erase(const std::shared_ptr<SegmentKey> &key);
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LRUCACHINGPOLICY_H
