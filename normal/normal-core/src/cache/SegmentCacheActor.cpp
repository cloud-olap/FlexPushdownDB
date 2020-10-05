//
// Created by matt on 21/5/20.
//

#include "normal/core/cache/SegmentCacheActor.h"

namespace normal::core::cache {

std::shared_ptr<LoadResponseMessage> SegmentCacheActor::load(const LoadRequestMessage &msg,
															 stateful_actor<SegmentCacheActorState> *self) {

  std::unordered_map<std::shared_ptr<SegmentKey>,
					 std::shared_ptr<SegmentData>,
					 SegmentKeyPointerHash,
					 SegmentKeyPointerPredicate> segments;
//  std::vector<std::shared_ptr<SegmentKey>> segmentKeysToCache;
  auto missSegmentKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

//  SPDLOG_DEBUG("Handling load request. loadRequestMessage: {}", msg.toString());

  for (const auto &segmentKey: msg.getSegmentKeys()) {

	auto cacheData = self->state.cache->load(segmentKey);

	if (cacheData.has_value()) {
	  SPDLOG_DEBUG("Cache hit  |  segmentKey: {}", segmentKey->toString());
	  segments.insert(std::pair(segmentKey, cacheData.value()));

	} else {
	  SPDLOG_DEBUG("Cache miss  |  segmentKey: {}", segmentKey->toString());
	  missSegmentKeys->emplace_back(segmentKey);
	}
  }

  auto segmentKeysToCache = self->state.cache->toCache(missSegmentKeys);

  return LoadResponseMessage::make(segments, self->state.name, *segmentKeysToCache);
}

void SegmentCacheActor::store(const StoreRequestMessage &msg,
							  stateful_actor<SegmentCacheActorState> *self) {
//  SPDLOG_DEBUG("Store  |  storeMessage: {}", msg.toString());
  for (const auto &segmentEntry: msg.getSegments()) {
	auto segmentKey = segmentEntry.first;
	auto segmentData = segmentEntry.second;
	self->state.cache->store(segmentKey, segmentData);
  }
}

behavior SegmentCacheActor::makeBehaviour(stateful_actor<SegmentCacheActorState> *self,
										  const std::optional<std::shared_ptr<CachingPolicy>> &cachingPolicy) {

  if (cachingPolicy.has_value())
	self->state.cache = SegmentCache::make(cachingPolicy.value());
  else
	self->state.cache = SegmentCache::make();

  return {
	  [=](LoadAtom, const std::shared_ptr<LoadRequestMessage> &m) {
		return load(*m, self);
	  },
	  [=](StoreAtom, const std::shared_ptr<StoreRequestMessage> &m) {
		store(*m, self);
	  },
	  [=](GetNumHitsAtom) {
		return self->state.cache->hitNum();
	  },
	  [=](GetNumMissesAtom) {
		return self->state.cache->missNum();
	  },
	  [=](ClearMetricsAtom) {
		self->state.cache->clearMetrics();
	  }
  };
}

}
