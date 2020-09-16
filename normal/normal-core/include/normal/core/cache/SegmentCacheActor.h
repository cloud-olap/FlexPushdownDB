//
// Created by matt on 21/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H

#include <normal/core/Operator.h>

#include <caf/all.hpp>

#include <normal/cache/SegmentCache.h>

#include <normal/core/Globals.h>

#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/StoreRequestMessage.h>
#include <normal/core/cache/EvictRequestMessage.h>
#include "SegmentCacheActorState.h"
#include "LoadResponseMessage.h"
#include <normal/core/Forward.h>

using namespace caf;
using namespace normal::cache;

namespace normal::core::cache {

using LoadAtom = caf::atom_constant<atom("Load")>;
using StoreAtom = caf::atom_constant<atom("Store")>;
using GetNumHitsAtom = caf::atom_constant<atom("NumHits")>;
using GetNumMissesAtom = caf::atom_constant<atom("NumMisses")>;
using ClearMetricsAtom = caf::atom_constant<atom("ClrMetrics")>;

class SegmentCacheActor : public caf::event_based_actor {

public:

  explicit SegmentCacheActor(caf::actor_config &cfg);
  explicit SegmentCacheActor(caf::actor_config &cfg, const std::shared_ptr<CachingPolicy>& cachingPolicy);

  caf::behavior make_behavior() override {
	return {
		[=](const normal::core::message::Envelope &message) {
//		  if (message.message().type() == "StoreRequestMessage") {
//			auto storeMessage = dynamic_cast<const StoreRequestMessage &>(message.message());
//			store(storeMessage);
//		  } else if (message.message().type() == "LoadRequestMessage") {
//			auto loadMessage = dynamic_cast<const LoadRequestMessage &>(message.message());
//			load(loadMessage);
		  if (message.message().type() == "EvictRequestMessage") {
			auto evictMessage = dynamic_cast<const EvictRequestMessage &>(message.message());
			evict(evictMessage);
		  } else if (message.message().type() == "StartMessage") {
			auto startMessage = dynamic_cast<const StartMessage &>(message.message());
			// NOOP
		  } else {
			// FIXME: Propagate error properly
			throw std::runtime_error("Unrecognized message type " + message.message().type());
		  }
		},
		[=](LoadAtom, const std::shared_ptr<LoadRequestMessage>& m){
		  return load(*m);
		},
		[=](StoreAtom, const std::shared_ptr<StoreRequestMessage>& m){
		  store(*m);
		},
		[=](GetNumHitsAtom){
		  return state_->cache->hitNum();
		},
		[=](GetNumMissesAtom){
		  return state_->cache->missNum();
		},
		[=](ClearMetricsAtom){
		  state_->cache->clearMetrics();
		}
	};
  }

//  void onReceive(const message::Envelope &message) override;

  std::shared_ptr<LoadResponseMessage> load(const LoadRequestMessage &msg);
  void store(const StoreRequestMessage &msg);
  void evict(const EvictRequestMessage &msg);

  [[nodiscard]] const std::shared_ptr<SegmentCacheActorState> &getState() const;

  void on_exit() override{
    state_->cache.reset();
  }

private:
  std::shared_ptr<SegmentCacheActorState> state_;

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::LoadResponseMessage>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::LoadRequestMessage>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<normal::core::cache::StoreRequestMessage>);

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
