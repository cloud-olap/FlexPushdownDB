//
// Created by matt on 21/5/20.
//

#include <fpdb/executor/cache/SegmentCacheActor.h>
#include <fpdb/cache/policy/WLFUCachingPolicy.h>
#include <spdlog/spdlog.h>

namespace fpdb::executor::cache {

std::shared_ptr<LoadResponseMessage> SegmentCacheActor::load(const LoadRequestMessage &msg,
                                                             stateful_actor<SegmentCacheActorState> *self,
                                                             const std::shared_ptr<Mode> &mode) {

  SPDLOG_DEBUG("Handling load request. loadRequestMessage: {}", msg.toString());

  std::unordered_map<std::shared_ptr<SegmentKey>,
					 std::shared_ptr<SegmentData>,
					 SegmentKeyPointerHash,
					 SegmentKeyPointerPredicate> segments;

  auto missSegmentKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

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


  // with no pushdown, all missing segments are required to load no matter we will cache them or not
  auto segmentKeysToCache = mode->id() == CACHING_ONLY ?
          missSegmentKeys : self->state.cache->toCache(missSegmentKeys);

  return LoadResponseMessage::make(segments, self->state.name, *segmentKeysToCache);
}

void SegmentCacheActor::store(const StoreRequestMessage &msg, stateful_actor<SegmentCacheActorState> *self) {
  SPDLOG_DEBUG("Store  |  storeMessage: {}", msg.toString());
  for (const auto &segmentEntry: msg.getSegments()) {
	auto segmentKey = segmentEntry.first;
	auto segmentData = segmentEntry.second;
	self->state.cache->store(segmentKey, segmentData);
  }
}

void SegmentCacheActor::weight(const WeightRequestMessage &msg, stateful_actor<SegmentCacheActorState> *self) {
  auto cachingPolicy = self->state.cache->getCachingPolicy();
  if (cachingPolicy->getType() == WLFU) {
    auto wlfuCachingPolicy = std::static_pointer_cast<WLFUCachingPolicy>(cachingPolicy);
    wlfuCachingPolicy->onWeight(msg.getWeightMap());
  }
}

void SegmentCacheActor::metrics(const CacheMetricsMessage &msg, stateful_actor<SegmentCacheActorState> *self) {
  self->state.cache->addHitNum(msg.getHitNum());
  self->state.cache->addMissNum(msg.getMissNum());
  self->state.cache->addShardHitNum(msg.getShardHitNum());
  self->state.cache->addShardMissNum(msg.getShardMissNum());

  self->state.cache->addCrtQueryHitNum(msg.getHitNum());
  self->state.cache->addCrtQueryMissNum(msg.getMissNum());
  self->state.cache->addCrtQueryShardHitNum(msg.getShardHitNum());
  self->state.cache->addCrtQueryShardMissNum(msg.getShardMissNum());
}

void SegmentCacheActor::stop(stateful_actor<SegmentCacheActorState> *self) {
  self->state.cache->clear();
  self->state.cache.reset();
}

behavior SegmentCacheActor::makeBehaviour(stateful_actor<SegmentCacheActorState> *self,
                                          std::shared_ptr<CachingPolicy> cachingPolicy,
                                          std::shared_ptr<Mode> mode) {

  if (cachingPolicy != nullptr)
	  self->state.cache = SegmentCache::make(std::move(cachingPolicy));
  else
	  throw runtime_error("Error when creating SegmentCacheActor: no caching policy specified");

  /**
   * Handler for actor exit event
   */
  self->attach_functor([=](const ::caf::error &reason) {

    SPDLOG_DEBUG("[Actor {} ('<name unavailable>')]  Segment Cache exit  |  reason: {}",
          self->id(),
           to_string(reason));

    // Important: we cannot do clear/release here which will throw "corrupted chunks" error,
    // which is probably because CAF already does some memory freeing before entering this exit function.
    // We need to explicitly make a "stop" api to clear.

  });

  return {
	  [=](LoadAtom, const std::shared_ptr<LoadRequestMessage> &m) {
		return load(*m, self, mode);
	  },
	  [=](StoreAtom, const std::shared_ptr<StoreRequestMessage> &m) {
		store(*m, self);
	  },
	  [=](WeightAtom, const std::shared_ptr<WeightRequestMessage> &m) {
		weight(*m, self);
	  },
    [=](NewQueryAtom) {
    self->state.cache->newQuery();
	  },
    [=](StopCacheAtom) {
    stop(self);
    },
	  [=](GetNumHitsAtom) {
		return self->state.cache->hitNum();
	  },
	  [=](GetNumMissesAtom) {
		return self->state.cache->missNum();
	  },
	  [=](GetNumShardHitsAtom) {
		return self->state.cache->shardHitNum();
	  },
	  [=](GetNumShardMissesAtom) {
		return self->state.cache->shardMissNum();
	  },
	  [=](GetCrtQueryNumHitsAtom) {
		return self->state.cache->crtQueryHitNum();
	  },
	  [=](GetCrtQueryNumMissesAtom) {
		return self->state.cache->crtQueryMissNum();
	  },
	  [=](GetCrtQueryNumShardHitsAtom) {
		return self->state.cache->crtQueryShardHitNum();
	  },
	  [=](GetCrtQueryNumShardMissesAtom) {
		return self->state.cache->crtQueryShardMissNum();
	  },
	  [=](ClearMetricsAtom) {
		self->state.cache->clearMetrics();
	  },
	  [=](MetricsAtom, const std::shared_ptr<CacheMetricsMessage> &m) {
    metrics(*m, self);
	  }
  };
}

}
