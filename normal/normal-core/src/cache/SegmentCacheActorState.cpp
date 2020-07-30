//
// Created by matt on 21/5/20.
//

#include "normal/core/cache/SegmentCacheActorState.h"

using namespace normal::core::cache;

std::shared_ptr<SegmentCacheActorState> normal::core::cache::make() {
  auto state = std::make_shared<SegmentCacheActorState>();
  state->cache = SegmentCache::make();
  return state;
}

std::shared_ptr<SegmentCacheActorState> normal::core::cache::make(const std::shared_ptr<CachingPolicy> &cachingPolicy) {
  auto state = std::make_shared<SegmentCacheActorState>();
  state->cache = SegmentCache::make(cachingPolicy);
  return state;
}
