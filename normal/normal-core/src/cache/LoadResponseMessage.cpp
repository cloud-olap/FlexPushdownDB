//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/LoadResponseMessage.h"

using namespace normal::core::cache;

LoadResponseMessage::LoadResponseMessage(std::shared_ptr<SegmentKey> segmentKey,
										 std::optional<std::shared_ptr<SegmentData>> segmentData) :
	segmentKey_(std::move(segmentKey)),
	segmentData_(std::move(segmentData)) {}

std::shared_ptr<LoadResponseMessage>
LoadResponseMessage::make(std::shared_ptr<SegmentKey> segmentKey,
						  std::optional<std::shared_ptr<SegmentData>> segmentData) {
  return std::make_shared<LoadResponseMessage>(std::move(segmentKey), std::move(segmentData));
}

[[maybe_unused]] const std::shared_ptr<SegmentKey> &LoadResponseMessage::getSegmentKey() const {
  return segmentKey_;
}

[[maybe_unused]] const std::optional<std::shared_ptr<SegmentData>> &LoadResponseMessage::getSegmentData() const {
  return segmentData_;
}

std::string LoadResponseMessage::toString() const {
  return fmt::format("segmentKey: {}, segmentData.size: {}", segmentKey_->toString(), 0);
}
