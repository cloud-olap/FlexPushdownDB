//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/LoadRequestMessage.h"

#include <utility>

using namespace normal::core::cache;

LoadRequestMessage::LoadRequestMessage(std::shared_ptr<SegmentKey> segmentKey) :
	segmentKey_(std::move(segmentKey)) {}

std::shared_ptr<LoadRequestMessage> LoadRequestMessage::make(std::shared_ptr<SegmentKey> segmentKey) {
  return std::make_shared<LoadRequestMessage>(std::move(segmentKey));
}

const std::shared_ptr<SegmentKey> &LoadRequestMessage::getSegmentKey() const {
  return segmentKey_;
}

std::string LoadRequestMessage::toString() const {
  return fmt::format("segmentKey: {}", segmentKey_->toString());
}




