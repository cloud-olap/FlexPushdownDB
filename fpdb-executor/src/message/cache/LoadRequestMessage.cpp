//
// Created by matt on 20/5/20.
//

#include <fpdb/executor/message/cache/LoadRequestMessage.h>
#include <fmt/format.h>
#include <utility>

using namespace fpdb::executor::message;

LoadRequestMessage::LoadRequestMessage(std::vector<std::shared_ptr<SegmentKey>> segmentKeys,
									   const std::string &sender) :
	Message(LOAD_REQUEST, sender),
	segmentKeys_(std::move(segmentKeys)) {}

std::shared_ptr<LoadRequestMessage> LoadRequestMessage::make(const std::vector<std::shared_ptr<SegmentKey>>& segmentKeys,
															 const std::string &sender) {
  return std::make_shared<LoadRequestMessage>(segmentKeys, sender);
}

std::string LoadRequestMessage::getTypeString() const {
  return "LoadRequestMessage";
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
