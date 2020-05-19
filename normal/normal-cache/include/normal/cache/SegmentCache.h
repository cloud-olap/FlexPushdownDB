//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H

#include <unordered_map>
#include <memory>

#include "SegmentKey.h"
#include "SegmentData.h"
#include "SegmentCacheEntry.h"

namespace normal::cache {

class SegmentCache {

public:
  SegmentCache();

  static std::shared_ptr<SegmentCache> make();

  void store(const std::shared_ptr<SegmentKey>& key, const std::shared_ptr<SegmentData>& data);
  tl::expected<std::shared_ptr<SegmentCacheEntry>, std::string> load(const std::shared_ptr<SegmentKey>& key);

  unsigned long erase(const std::shared_ptr<SegmentKey>& key);
  unsigned long erase(const std::function<bool(const SegmentCacheEntry& entry)>& predicate);

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentCacheEntry>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> map_;

};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H
