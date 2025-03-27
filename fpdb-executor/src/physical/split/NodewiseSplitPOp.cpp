//
// Created by Yifei Yang on 3/16/24.
//

#include <fpdb/executor/physical/split/NodewiseSplitPOp.h>
#include <fpdb/executor/physical/split/SplitKernel.h>
#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor::physical::split {

NodewiseSplitPOp::NodewiseSplitPOp(const std::string &name,
                                   const std::vector<std::string> &projectColumnNames,
                                   int nodeId,
                                   int numNodes):
  PhysicalOp(name, NODEWISE_SPLIT, projectColumnNames, nodeId),
  numNodes_(numNodes) {
  consumerGroups_.resize(numNodes);
}

std::string NodewiseSplitPOp::getTypeString() const {
  return "NodewiseSplitPOp";
}

void NodewiseSplitPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
    this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(msg.message());
    this->onTupleSet(tupleSetMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}", msg.message().getTypeString(), name()));
  }
}

void NodewiseSplitPOp::produce(const std::shared_ptr<PhysicalOp> &op, int group) {
  PhysicalOp::produce(op);
  if (group >= numNodes_) {
    throw std::runtime_error(fmt::format("Invalid group '{}' when adding consumer to NodewiseSplitPOp, "
                                         "where numNodes_ = '{}'", group, numNodes_));
  }
  consumerGroups_[group].emplace_back(op->name());
}

void NodewiseSplitPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void NodewiseSplitPOp::onTupleSet(const TupleSetMessage &message) {
  if (inputGroups_.empty()) {
    initInputBuffers();
  }

  // get tupleSet and its corresponding incoming node
  const auto &tupleSet = message.tuples();
  const auto &sender = message.sender();
  const auto &senderOpEntry = ctx()->operatorMap().get(sender);
  if (!senderOpEntry.has_value()) {
    ctx()->notifyError(senderOpEntry.error());
    return;
  }
  int group = (*senderOpEntry).getNodeId();

  // buffer
  auto res = bufferInput(tupleSet, group);
  if (!res.has_value()) {
    ctx()->notifyError(res.error());
    return;
  }

  // process if buffer is large enough
  const auto &buffer = inputGroups_[group];
  if (buffer != nullptr && buffer->numRows() >= DefaultBufferSize * (int) consumerGroups_[group].size()) {
    res = splitAndSend(group);
    if (!res.has_value()) {
      ctx()->notifyError(res.error());
      return;
    }
  }
}

void NodewiseSplitPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    if (inputGroups_.empty()) {
      ctx()->notifyError("No input received");
      return;
    }
    for (int group = 0; group < numNodes_; ++group) {
      auto &inputTupleSet = inputGroups_[group];
      bool sentResult = sentResultGroups_[group];
      if (inputTupleSet != nullptr && (!sentResult || inputTupleSet->numRows() > 0)) {
        auto res = splitAndSend(group);
        if (!res.has_value()) {
          ctx()->notifyError(res.error());
          return;
        }
      }
    }

    ctx()->notifyComplete();
  }
}

tl::expected<void, string> NodewiseSplitPOp::splitAndSend(int group) {
  // check input
  auto &buffer = inputGroups_[group];
  if (buffer == nullptr) {
    return tl::make_unexpected("No input tupleSet to split");
  }

  // split
  const auto &expTupleSets = SplitKernel::split2(buffer, consumerGroups_[group].size());
  if (!expTupleSets.has_value()) {
    return tl::make_unexpected(expTupleSets.error());
  }

  // send
  send(expTupleSets.value(), group);
  sentResultGroups_[group] = true;
  buffer = nullptr;
  return {};
}

tl::expected<void, string> NodewiseSplitPOp::bufferInput(const shared_ptr<TupleSet>& tupleSet, int group) {
  auto &buffer = inputGroups_[group];
  if (buffer == nullptr) {
    buffer = tupleSet;
  } else {
    auto expConcatenatedTupleSet = TupleSet::concatenate({buffer, tupleSet});
    if (!expConcatenatedTupleSet.has_value()) {
      return tl::make_unexpected(expConcatenatedTupleSet.error());
    }
    buffer = *expConcatenatedTupleSet;
  }
  return {};
}

void NodewiseSplitPOp::send(const std::vector<shared_ptr<TupleSet>> &tupleSets, int group) {
  auto &consumerGroup = consumerGroups_[group];
  for (uint i = 0; i < consumerGroup.size(); ++i) {
    const auto &consumer = consumerGroup[i];
    const auto &tupleSet = tupleSets[i];
    shared_ptr<Message> tupleSetMessage = make_shared<TupleSetMessage>(tupleSet, name_);
    ctx()->send(tupleSetMessage, consumer);
  }
}

void NodewiseSplitPOp::initInputBuffers() {
  inputGroups_.resize(numNodes_);
  sentResultGroups_.resize(numNodes_);
}

void NodewiseSplitPOp::clear() {
  inputGroups_.clear();
}

}
