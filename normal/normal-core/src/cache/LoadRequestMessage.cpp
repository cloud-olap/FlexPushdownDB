//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/LoadRequestMessage.h"

#include <utility>

using namespace normal::core::cache;

LoadRequestMessage::LoadRequestMessage(std::shared_ptr<SegmentKey> segmentKey,
									   const std::string &sender) :
	Message("LoadRequestMessage", sender),
	segmentKey_(std::move(segmentKey)) {}

std::shared_ptr<LoadRequestMessage> LoadRequestMessage::make(std::shared_ptr<SegmentKey> segmentKey,
															 const std::string &sender) {
  return std::make_shared<LoadRequestMessage>(std::move(segmentKey), sender);
}

const std::shared_ptr<SegmentKey> &LoadRequestMessage::getSegmentKey() const {
  return segmentKey_;
}

std::string LoadRequestMessage::toString() const {
  return fmt::format("{{segmentKey: {}}}", segmentKey_->toString());
}
