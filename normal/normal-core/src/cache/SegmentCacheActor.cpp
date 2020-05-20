//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/SegmentCacheActor.h"

#include <normal/core/cache/LoadResponseMessage.h>

using namespace normal::core::cache;

SegmentCacheActor::behavior_type behaviour(SegmentCacheActor::stateful_pointer <SegmentCacheActorState> self) {
  return {
	  [&](const normal::core::cache::LoadRequestMessage &msg) {
		SPDLOG_TRACE("Load  |  loadMessage: {}", msg.toString());
		auto cacheEntry = self->state.cache_->load(msg.getSegmentKey());
		auto loadedMessage = cacheEntry.has_value() ?
							 LoadResponseMessage::make(msg.getSegmentKey(), std::nullopt) :
							 LoadResponseMessage::make(msg.getSegmentKey(), std::optional(cacheEntry.value()->getData()));

		// TODO: Send response

	  },
	  [&](const normal::core::cache::StoreRequestMessage &msg) {
		SPDLOG_TRACE("Store  |  storeMessage: {}", msg.toString());
		self->state.cache_->store(msg.getSegmentKey(), msg.getSegmentData());
	  },
	  [&](const normal::core::cache::EvictRequestMessage &msg) {
		SPDLOG_TRACE("Evict  |  evictMessage: {}", msg.toString());
		throw std::runtime_error("Cache eviction not implemented");
	  }
  };
}
