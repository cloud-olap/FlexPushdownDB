//
// Created by matt on 21/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H

#include <caf/all.hpp>

#include <normal/cache/SegmentCache.h>
#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/core/cache/StoreRequestMessage.h>
#include <normal/cache/CachingPolicy.h>
#include <normal/core/Forward.h>

using namespace caf;
using namespace normal::cache;

namespace normal::core::cache {

struct SegmentCacheActorState {
  std::string name = "segment-cache";
  std::shared_ptr<SegmentCache> cache;
};

using LoadAtom = atom_constant<atom("Load")>;
using StoreAtom = atom_constant<atom("Store")>;
using GetNumHitsAtom = atom_constant<atom("NumHits")>;
using GetNumMissesAtom = atom_constant<atom("NumMisses")>;
using ClearMetricsAtom = atom_constant<atom("ClrMetrics")>;

class SegmentCacheActor {

public:
  static behavior makeBehaviour(stateful_actor<SegmentCacheActorState> *self,
								const std::optional<std::shared_ptr<CachingPolicy>> &cachingPolicy);

  static std::shared_ptr<LoadResponseMessage> load(const LoadRequestMessage &msg,
												   stateful_actor<SegmentCacheActorState> *self);
  static void store(const StoreRequestMessage &msg, stateful_actor<SegmentCacheActorState> *self);

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::LoadResponseMessage>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::LoadRequestMessage>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::StoreRequestMessage>);

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
