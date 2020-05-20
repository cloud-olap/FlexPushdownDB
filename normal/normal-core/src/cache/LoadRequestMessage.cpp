//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/LoadRequestMessage.h"

#include <utility>

using namespace normal::core::cache;

LoadRequestMessage::LoadRequestMessage(std::shared_ptr<SegmentKey> SegmentKey) : segmentKey_(std::move(SegmentKey)) {}

std::shared_ptr<LoadRequestMessage> LoadRequestMessage::make(const std::shared_ptr<SegmentKey> &SegmentKey) {
  return std::make_shared<LoadRequestMessage>(SegmentKey);
}

const std::shared_ptr<SegmentKey> &LoadRequestMessage::getSegmentKey() const {
  return segmentKey_;
}

std::string LoadRequestMessage::toString() {
  return fmt::format("segmentKey: {}", segmentKey_->toString());
}




