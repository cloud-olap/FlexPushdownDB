//
// Created by Yifei Yang on 9/9/20.
//

#include <fpdb/executor/message/cache/WeightRequestMessage.h>

using namespace fpdb::executor::message;

WeightRequestMessage::WeightRequestMessage(const std::unordered_map<std::shared_ptr<SegmentKey>, double> &weightMap,
                                           const std::string &sender) :
  Message(WEIGHT_REQUEST, sender),
  weightMap_(weightMap) {}

std::shared_ptr<WeightRequestMessage>
WeightRequestMessage::make(const std::unordered_map<std::shared_ptr<SegmentKey>, double> &weightMap,
                           const std::string &sender) {
  return std::make_shared<WeightRequestMessage>(weightMap, sender);
}

std::string WeightRequestMessage::getTypeString() const {
  return "WeightRequestMessage";
}

const std::unordered_map<std::shared_ptr<SegmentKey>, double> & WeightRequestMessage::getWeightMap() const {
  return weightMap_;
}
