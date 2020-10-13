//
// Created by matt on 21/5/20.
//

#include <normal/cache/WFBRCachingPolicy.h>
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

void SegmentCacheActor::weight(const WeightRequestMessage &msg,
							   stateful_actor<SegmentCacheActorState> *self) {
  auto cachingPolicy = self->state.cache->getCachingPolicy();
  if (cachingPolicy->id() == WFBR) {
	auto fbrCachingPolicy = std::static_pointer_cast<WFBRCachingPolicy>(cachingPolicy);
	fbrCachingPolicy->onWeight(msg.getWeightMap(), msg.getQueryId());
  }
}

behavior SegmentCacheActor::makeBehaviour(stateful_actor<SegmentCacheActorState> *self,
										  const std::optional<std::shared_ptr<CachingPolicy>> &cachingPolicy) {

  if (cachingPolicy.has_value())
	self->state.cache = SegmentCache::make(cachingPolicy.value());
  else
	self->state.cache = SegmentCache::make();

  /**
   * Handler for actor exit event
   */
  self->attach_functor([=](const caf::error &reason) {

	SPDLOG_DEBUG("[Actor {} ('{}')]  Segment Cache Actor exit  |  reason: {}", self->id(),
				 self->name(), to_string(reason));

	self->state.cache.reset();
  });

  return {
	  [=](LoadAtom, const std::shared_ptr<LoadRequestMessage> &m) {
		return load(*m, self);
	  },
	  [=](StoreAtom, const std::shared_ptr<StoreRequestMessage> &m) {
		store(*m, self);
	  },
	  [=](WeightAtom, const std::shared_ptr<WeightRequestMessage> &m) {
		weight(*m, self);
	  },
	  [=](GetNumHitsAtom) {
		return self->state.cache->hitNum();
	  },
	  [=](GetNumMissesAtom) {
		return self->state.cache->missNum();
	  },
	  [=](GetCrtQueryNumHitsAtom) {
		return self->state.cache->crtQueryHitNum();
	  },
	  [=](GetCrtQueryNumMissesAtom) {
		return self->state.cache->crtQueryMissNum();
	  },
	  [=](ClearMetricsAtom) {
		self->state.cache->clearMetrics();
	  },
	  [=](ClearCrtQueryMetricsAtom) {
		self->state.cache->clearCrtQueryMetrics();
	  }
  };
}

}
