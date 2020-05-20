//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/LoadResponseMessage.h"

using namespace normal::core::cache;

LoadResponseMessage::LoadResponseMessage(std::shared_ptr<SegmentKey> SegmentKey,
										 std::optional<std::shared_ptr<SegmentData>> SegmentData)
	: segmentKey_(std::move(SegmentKey)), segmentData_(std::move(SegmentData)) {}

std::shared_ptr<LoadResponseMessage> LoadResponseMessage::make(std::shared_ptr<SegmentKey> SegmentKey,
															   std::optional<std::shared_ptr<SegmentData>> SegmentData) {
  return std::make_shared<LoadResponseMessage>(std::move(SegmentKey), std::move(SegmentData));
}

const std::shared_ptr<SegmentKey> &LoadResponseMessage::getSegmentKey() const {
  return segmentKey_;
}

const std::optional<std::shared_ptr<SegmentData>> &LoadResponseMessage::getSegmentData() const {
  return segmentData_;
}

std::string LoadResponseMessage::toString() {
  return fmt::format("segmentKey: {}, segmentData.size: {}", segmentKey_->toString(), 0);
}
