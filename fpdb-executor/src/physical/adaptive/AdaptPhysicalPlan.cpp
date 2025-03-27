//
// Created by Yifei Yang on 4/5/24.
//

#include <fpdb/executor/physical/adaptive/AdaptPhysicalPlan.h>

namespace fpdb::executor::physical {

AdaptPhysicalPlan::AdaptPhysicalPlan(const std::unordered_map<std::string, std::shared_ptr<PhysicalOp>> &physicalOps,
                                     const std::unordered_set<std::string> &sinkToProduce,
                                     const std::unordered_map<std::string, bool> &sinkToConsume,
                                     bool consumeAllSink,
                                     const std::string &rootPOpName):
  PhysicalPlan(physicalOps, rootPOpName),
  sinkToProduce_(sinkToProduce),
  sinkToConsume_(sinkToConsume),
  consumeAllSink_(consumeAllSink) {}

size_t AdaptPhysicalPlan::numSinkToProduce() const {
  return sinkToProduce_.size();
}

size_t AdaptPhysicalPlan::numSinkToConsume() const {
  return sinkToConsume_.size();
}

size_t AdaptPhysicalPlan::numSinkToComplete() const {
  size_t cnt = 0;
  for (const auto &it: sinkToConsume_) {
    cnt += !it.second;
  }
  return cnt;
}

bool AdaptPhysicalPlan::isSinkToProduce(const std::string &op) const {
  return sinkToProduce_.find(op) != sinkToProduce_.end();
}

bool AdaptPhysicalPlan::isSinkToConsume(const std::string &op) const {
  return sinkToConsume_.find(op) != sinkToConsume_.end();
}

bool AdaptPhysicalPlan::isSinkToConsume(const std::string &op, bool* preserve) const {
  auto it = sinkToConsume_.find(op);
  if (it == sinkToConsume_.end()) {
    return false;
  } else {
    *preserve = it->second;
    return true;
  }
}

bool AdaptPhysicalPlan::consumeAllSink() const {
  return consumeAllSink_;
}

}
