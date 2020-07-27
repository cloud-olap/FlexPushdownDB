//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/LoadResponseMessage.h"

using namespace normal::core::cache;

LoadResponseMessage::LoadResponseMessage(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> segments,
										 const std::string &sender) :
	Message("LoadResponseMessage", sender),
	segments_(std::move(segments)) {}

std::shared_ptr<LoadResponseMessage>

LoadResponseMessage::make(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> segments,
						  const std::string &sender) {
  return std::make_shared<LoadResponseMessage>(std::move(segments), sender);
}

[[maybe_unused]] const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> &LoadResponseMessage::getSegments() const {
  return segments_;
}

std::string LoadResponseMessage::toString() const {

  std::string s = "{";
  for(auto it = segments_.begin();it != segments_.end();++it){
	s += fmt::format("{}", it->first->toString());
	s += ": ";
	s += fmt::format("{}", it->second->getColumn()->toString());
	if(std::next(it) != segments_.end())
	  s +=", ";
  }
  s += "}";

  return s;
}
