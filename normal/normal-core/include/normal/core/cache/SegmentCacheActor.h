//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H

#include <caf/all.hpp>

#include <normal/cache/SegmentCache.h>

#include <normal/core/Globals.h>

#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/StoreRequestMessage.h>
#include <normal/core/cache/EvictRequestMessage.h>

using namespace caf;
using namespace normal::cache;

namespace normal::core::cache {

/**
 * Actor guarding access to a segment cache.
 *
 * Manages a single instance of a segment cache. Provides store, load and evict behaviours.
 */
using SegmentCacheActor = typed_actor<reacts_to<normal::core::cache::LoadRequestMessage>,
									  reacts_to<normal::core::cache::StoreRequestMessage>,
									  reacts_to<normal::core::cache::EvictRequestMessage>>;

struct SegmentCacheActorState {
  std::shared_ptr<SegmentCache> cache_ = SegmentCache::make();
};

SegmentCacheActor::behavior_type segmentCacheActorBehaviour(SegmentCacheActor::stateful_pointer <SegmentCacheActorState> self);

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
