//
// Created by Yifei Yang on 9/9/20.
//

#include <normal/core/cache/WeightRequestMessage.h>

using namespace normal::core::cache;

WeightRequestMessage::WeightRequestMessage(const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> weightMap,
                                           long queryId,
                                           const std::string &sender) :
  Message("WeightRequestMessage", sender),
  weightMap_(weightMap),
  queryId_(queryId) {}

std::shared_ptr<WeightRequestMessage> WeightRequestMessage::make(const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> weightMap,
                                                                 long queryId,
                                                                 const std::string &sender) {
  return std::make_shared<WeightRequestMessage>(weightMap, queryId, sender);
}

long WeightRequestMessage::getQueryId() const {
  return queryId_;
}

const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &
WeightRequestMessage::getWeightMap() const {
  return weightMap_;
}
