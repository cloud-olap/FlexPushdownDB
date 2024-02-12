//
// Created by Yifei Yang on 12/12/21.
//

#include <fpdb/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h>
#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor::physical::join {

NestedLoopJoinPOp::NestedLoopJoinPOp(const string &name,
                                     const vector<string> &projectColumnNames,
                                     int nodeId,
                                     const std::optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                     JoinType joinType) :
  PhysicalOp(name, NESTED_LOOP_JOIN, projectColumnNames, nodeId),
  kernel_(makeKernel(predicate, joinType)) {}

NestedLoopJoinKernel
NestedLoopJoinPOp::makeKernel(const std::optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                              JoinType joinType) {
  set<string> neededColumnNames(projectColumnNames_.begin(), projectColumnNames_.end());
  if (predicate.has_value()) {
    const auto &predInvolvedColumnNames = (*predicate)->involvedColumnNames();
    neededColumnNames.insert(predInvolvedColumnNames.begin(), predInvolvedColumnNames.end());
  }
  switch (joinType) {
    case INNER: {
      return NestedLoopJoinKernel::make(predicate, neededColumnNames, false, false);
    }
    case LEFT: {
      return NestedLoopJoinKernel::make(predicate, neededColumnNames, true, false);
    }
    case RIGHT: {
      return NestedLoopJoinKernel::make(predicate, neededColumnNames, false, true);
    }
    case FULL: {
      return NestedLoopJoinKernel::make(predicate, neededColumnNames, true, true);
    }
    default:
      // This is not inside actor processing, so we cannot use ctx()->notifyError()
      throw runtime_error(fmt::format("Unsupported nested loop join type, {}", joinType));
  }
}

std::string NestedLoopJoinPOp::getTypeString() const {
  return "NestedLoopJoinPOp";
}

void NestedLoopJoinPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
    this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET) {
    auto tupleMessage = dynamic_cast<const TupleSetMessage &>(msg.message());
    this->onTupleSet(tupleMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + msg.message().getTypeString());
  }
}

void NestedLoopJoinPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void NestedLoopJoinPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    // Finalize
    auto result = kernel_.finalize();
    if (!result) {
      ctx()->notifyError(result.error());
    }

    // Send final tupleSet
    send(true);

    // Send empty if no result
    if (!sentResult) {
      sendEmpty();
    }

    // Complete
    ctx()->notifyComplete();
  }
}

void NestedLoopJoinPOp::onTupleSet(const TupleSetMessage &message) {
  const auto &tupleSet = message.tuples();
  const auto &sender = message.sender();

  // incremental join immediately
  tl::expected<void, string> result;
  if (leftProducers_.find(sender) != leftProducers_.end()) {
    result = kernel_.joinIncomingLeft(tupleSet);
  } else if (rightProducers_.find(sender) != rightProducers_.end()) {
    result = kernel_.joinIncomingRight(tupleSet);
  } else {
    ctx()->notifyError(fmt::format("Unknown sender '{}', neither left nor right producer", sender));
  }
  if (!result.has_value()) {
    ctx()->notifyError(result.error());
  }

  // send result if exceeding buffer size
  send(false);
}

void NestedLoopJoinPOp::addLeftProducer(const shared_ptr<PhysicalOp> &leftProducer) {
  leftProducers_.emplace(leftProducer->name());
  consume(leftProducer);
}

void NestedLoopJoinPOp::addRightProducer(const shared_ptr<PhysicalOp> &rightProducer) {
  rightProducers_.emplace(rightProducer->name());
  consume(rightProducer);
}

void NestedLoopJoinPOp::send(bool force) {
  auto buffer = kernel_.getBuffer();
  if (buffer.has_value()) {
    auto numRows = buffer.value()->numRows();
    if (numRows >= DefaultBufferSize || (force && numRows > 0)) {
      // Project using projectColumnNames
      auto expProjectTupleSet = TupleSet::make(buffer.value()->table())->projectExist(getProjectColumnNames());
      if (!expProjectTupleSet) {
        ctx()->notifyError(expProjectTupleSet.error());
      }

      shared_ptr<Message> tupleSetMessage = make_shared<TupleSetMessage>(expProjectTupleSet.value(), name());
      ctx()->tell(tupleSetMessage);
      sentResult = true;
      kernel_.clearBuffer();
    }
  }
}

void NestedLoopJoinPOp::sendEmpty() {
  auto outputSchema = kernel_.getOutputSchema();
  if (!outputSchema.has_value()) {
    ctx()->notifyError("OutputSchema not set yet");
  }
  auto expProjectTupleSet = TupleSet::make(outputSchema.value())->projectExist(getProjectColumnNames());
  if (!expProjectTupleSet.has_value()) {
    ctx()->notifyError(expProjectTupleSet.error());
  }

  shared_ptr<Message> tupleSetMessage = make_shared<TupleSetMessage>(expProjectTupleSet.value(), name());
  ctx()->tell(tupleSetMessage);
}

void NestedLoopJoinPOp::clear() {
  kernel_.clear();
}

}
