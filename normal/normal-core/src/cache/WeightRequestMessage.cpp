//
// Created by Yifei Yang on 9/9/20.
//

#include <normal/core/cache/WeightRequestMessage.h>

using namespace normal::core::cache;

WeightRequestMessage::WeightRequestMessage(const std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> &segmentKeys,
                                           double weight,
                                           long queryId,
                                           const std::string &sender) :
  Message("WeightRequestMessage", sender),
  segmentKeys_(segmentKeys),
  weight_(weight),
  queryId_(queryId) {}

std::shared_ptr<WeightRequestMessage> WeightRequestMessage::make(const std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> &segmentKeys,
                                                                 double weight,
                                                                 long queryId,
                                                                 const std::string &sender) {
  return std::make_shared<WeightRequestMessage>(segmentKeys, weight, queryId, sender);
}

const std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> &WeightRequestMessage::getSegmentKeys() const {
  return segmentKeys_;
}

double WeightRequestMessage::getWeight() const {
  return weight_;
}

long WeightRequestMessage::getQueryId() const {
  return queryId_;
}
