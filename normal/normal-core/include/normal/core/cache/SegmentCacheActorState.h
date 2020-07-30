//
// Created by matt on 21/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTORSTATE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTORSTATE_H

#include <memory>
#include <normal/cache/SegmentCache.h>

using namespace normal::cache;

namespace normal::core::cache {

struct SegmentCacheActorState {
  std::shared_ptr<SegmentCache> cache;
};

std::shared_ptr<SegmentCacheActorState> make();
std::shared_ptr<SegmentCacheActorState> make(const std::shared_ptr<CachingPolicy>& cachingPolicy);

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTORSTATE_H
