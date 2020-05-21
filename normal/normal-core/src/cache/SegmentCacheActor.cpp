//
// Created by matt on 21/5/20.
//

#include "normal/core/cache/SegmentCacheActor.h"

using namespace normal::core::cache;

SegmentCacheActor::SegmentCacheActor(const std::string &Name) :
	Operator(Name, "SegmentCache"),
	state_(std::make_shared<SegmentCacheActorState>()) {}

void SegmentCacheActor::onReceive(const Envelope &message) {
  if (message.message().type() == "StoreRequestMessage") {
	auto storeMessage = dynamic_cast<const StoreRequestMessage &>(message.message());
	store(storeMessage);
  } else if (message.message().type() == "LoadRequestMessage") {
	auto loadMessage = dynamic_cast<const LoadRequestMessage &>(message.message());
	load(loadMessage);
  } else if (message.message().type() == "CompleteMessage") {
	auto evictMessage = dynamic_cast<const EvictRequestMessage &>(message.message());
	evict(evictMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void SegmentCacheActor::load(const LoadRequestMessage &msg) {

  SPDLOG_TRACE("Load  |  loadMessage: {}", msg.toString());

  auto cacheEntry = state_->cache->load(msg.getSegmentKey());

  auto loadedMessage = cacheEntry.has_value() ?
					   LoadResponseMessage::make(msg.getSegmentKey(), std::nullopt, name()) :
					   LoadResponseMessage::make(msg.getSegmentKey(),
												 std::optional(cacheEntry.value()->getData()),
												 name());

  // TODO: Send response
}

void SegmentCacheActor::store(const StoreRequestMessage &msg) {
  SPDLOG_TRACE("Store  |  storeMessage: {}", msg.toString());
  state_->cache->store(msg.getSegmentKey(), msg.getSegmentData());
}

void SegmentCacheActor::evict(const EvictRequestMessage &msg) {
  SPDLOG_TRACE("Evict  |  evictMessage: {}", msg.toString());
  throw std::runtime_error("Cache eviction not implemented");
}
