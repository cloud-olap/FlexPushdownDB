//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/StoreRequestMessage.h"

using namespace normal::core::cache;

StoreRequestMessage::StoreRequestMessage(
	std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments,
	const std::string &sender,
	bool used) :
	Message("StoreRequestMessage", sender),
	segments_(std::move(segments)),
	used_(used) {}

std::shared_ptr<StoreRequestMessage>
StoreRequestMessage::make(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments,
						  const std::string &sender,
						  bool used) {
  return std::make_shared<StoreRequestMessage>(std::move(segments), sender, used);
}

const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> &
StoreRequestMessage::getSegments() const {
  return segments_;
}

std::string StoreRequestMessage::toString() const {
  std::string s = "{";
  for (auto it = segments_.begin(); it != segments_.end(); ++it) {
	s += fmt::format("{}", it->first->toString());
	s += ": ";
	s += fmt::format("{}", it->second->getColumn()->toString());
	if (std::next(it) != segments_.end())
	  s += ", ";
  }
  s += "}";

  return s;
}

bool StoreRequestMessage::used() const {
  return used_;
}
