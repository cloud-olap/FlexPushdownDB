//
// Created by matt on 20/5/20.
//

#include <fpdb/executor/message/cache/StoreRequestMessage.h>
#include <fmt/format.h>
#include <utility>

using namespace fpdb::executor::message;

StoreRequestMessage::StoreRequestMessage(std::unordered_map<std::shared_ptr<SegmentKey>,
        std::shared_ptr<SegmentData>> segments,
        const std::string &sender) :
	Message(STORE_REQUEST, sender),
	segments_(std::move(segments)) {}

std::shared_ptr<StoreRequestMessage>
StoreRequestMessage::make(const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>>& segments,
						  const std::string &sender) {
  return std::make_shared<StoreRequestMessage>(segments, sender);
}

std::string StoreRequestMessage::getTypeString() const {
  return "StoreRequestMessage";
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
