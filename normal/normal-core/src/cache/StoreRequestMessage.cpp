//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/StoreRequestMessage.h"

#include <utility>

using namespace normal::core::cache;

StoreRequestMessage::StoreRequestMessage(std::shared_ptr<SegmentKey> SegmentKey,
										 std::shared_ptr<SegmentData> SegmentData) :
	segmentKey_(std::move(SegmentKey)),
	segmentData_(std::move(SegmentData)) {}

std::shared_ptr<StoreRequestMessage> StoreRequestMessage::make(std::shared_ptr<SegmentKey> SegmentKey,
															   std::shared_ptr<SegmentData> SegmentData) {
  return std::make_shared<StoreRequestMessage>(std::move(SegmentKey), std::move(SegmentData));
}

const std::shared_ptr<SegmentKey> &StoreRequestMessage::getSegmentKey() const {
  return segmentKey_;
}

const std::shared_ptr<SegmentData> &StoreRequestMessage::getSegmentData() const {
  return segmentData_;
}

std::string StoreRequestMessage::toString() const {
  return fmt::format("segmentKey: {}, segmentData.size: {}", segmentKey_->toString(), 0);
}
