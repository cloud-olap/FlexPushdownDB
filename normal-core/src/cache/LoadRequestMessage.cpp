//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/LoadRequestMessage.h"

#include <utility>

using namespace normal::core::cache;

LoadRequestMessage::LoadRequestMessage(std::vector<std::shared_ptr<SegmentKey>> segmentKeys,
									   const std::string &sender) :
	Message("LoadRequestMessage", sender),
	segmentKeys_(std::move(segmentKeys)) {}

std::shared_ptr<LoadRequestMessage> LoadRequestMessage::make(const std::vector<std::shared_ptr<SegmentKey>>& segmentKeys,
															 const std::string &sender) {
  return std::make_shared<LoadRequestMessage>(segmentKeys, sender);
}

const std::vector<std::shared_ptr<SegmentKey>> &LoadRequestMessage::getSegmentKeys() const {
  return segmentKeys_;
}

std::string LoadRequestMessage::toString() const {

  std::string s = "segmentKeys : [";
  for (auto it = segmentKeys_.begin(); it != segmentKeys_.end(); ++it) {
	s += fmt::format("{}", it->get()->toString());
	if (std::next(it) != segmentKeys_.end())
	  s += ",";
  }
  s += "]";

  return s;
}
