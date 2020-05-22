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
  } else if (message.message().type() == "EvictRequestMessage") {
	auto evictMessage = dynamic_cast<const EvictRequestMessage &>(message.message());
	evict(evictMessage);
  } else if (message.message().type() == "StartMessage") {
	auto startMessage = dynamic_cast<const StartMessage &>(message.message());
	// NOOP
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void SegmentCacheActor::load(const LoadRequestMessage &msg) {

  SPDLOG_DEBUG("Handling load request. loadRequestMessage: {}", msg.toString());

  auto cacheEntry = state_->cache->load(msg.getSegmentKey());

  std::shared_ptr<Message> responseMessage;
  if(cacheEntry.has_value()){
	SPDLOG_DEBUG("Cache hit. segmentKey: {}", msg.getSegmentKey()->toString());
	responseMessage = LoadResponseMessage::make(msg.getSegmentKey(),
											  std::optional(cacheEntry.value()->getData()),
											  name());
  }
  else{
	SPDLOG_DEBUG("Cache miss. segmentKey: {}", msg.getSegmentKey()->toString());
	responseMessage = LoadResponseMessage::make(msg.getSegmentKey(),
												std::nullopt,
												name());
  }

  ctx()->send(responseMessage, msg.sender())
	  .map_error([](auto err) { throw std::runtime_error(err); });
}

void SegmentCacheActor::store(const StoreRequestMessage &msg) {
  SPDLOG_DEBUG("Store  |  storeMessage: {}", msg.toString());
  state_->cache->store(msg.getSegmentKey(), msg.getSegmentData());
}

void SegmentCacheActor::evict(const EvictRequestMessage &msg) {
  SPDLOG_DEBUG("Evict  |  evictMessage: {}", msg.toString());
  throw std::runtime_error("Cache eviction not implemented");
}
