//
// Created by Yifei Yang on 12/13/21.
//

#include <fpdb/executor/physical/split/SplitPOp.h>
#include <fpdb/executor/physical/split/SplitKernel.h>
#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor::physical::split {

SplitPOp::SplitPOp(const string &name,
                   const vector<string> &projectColumnNames,
                   int nodeId):
  PhysicalOp(name, SPLIT, projectColumnNames, nodeId) {}

std::string SplitPOp::getTypeString() const {
  return "SplitPOp";
}

void SplitPOp::onReceive(const Envelope &msg) {
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

void SplitPOp::onStart() {
  SPDLOG_DEBUG("Starting '{}'  |  numConsumers: {}", name(), consumerVec_.size());
}

void SplitPOp::onTupleSet(const TupleSetMessage &message) {
  // get tupleSet
  const auto &tupleSet = message.tuples();
  numRows_ += tupleSet->numRows();
  const auto &result = bufferInput(tupleSet);
  if (!result.has_value()) {
    ctx()->notifyError(result.error());
  }

  // send if buffer is large enough
  if (inputTupleSet_.has_value() && inputTupleSet_.value()->numRows() >= DefaultBufferSize * (int) consumerVec_.size()) {
    auto res = splitAndSend();
    if (!res.has_value()) {
      ctx()->notifyError(res.error());
      return;
    }
  }
}

void SplitPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    if (inputTupleSet_.has_value() && (!sentResult_ || inputTupleSet_.value()->numRows() > 0)) {
      auto res = splitAndSend();
      if (!res.has_value()) {
        ctx()->notifyError(res.error());
        return;
      }
    }

    // record cardinality if needed
    sendPTCardMessage(ptCardInfo_, numRows_, std::nullopt);

    ctx()->notifyComplete();
  }
}

void SplitPOp::produce(const shared_ptr<PhysicalOp> &op) {
  PhysicalOp::produce(op);
  consumerVec_.emplace_back(op->name());
}

void SplitPOp::recordPredTransCard(const executor::cache::PredTransCardCache::PredTransCardKey &key) {
  ptCardInfo_.collect_ = true;
  ptCardInfo_.key_ = key;
}

tl::expected<void, string> SplitPOp::bufferInput(const shared_ptr<TupleSet>& tupleSet) {
  if (!inputTupleSet_.has_value()) {
    inputTupleSet_ = tupleSet;
  } else {
    auto expConcatenatedTupleSet = TupleSet::concatenate({*inputTupleSet_, tupleSet});
    if (!expConcatenatedTupleSet.has_value()) {
      return tl::make_unexpected(expConcatenatedTupleSet.error());
    }
    inputTupleSet_ = *expConcatenatedTupleSet;
  }
  return {};
}

tl::expected<void, string> SplitPOp::splitAndSend() {
  // check input
  if (!inputTupleSet_.has_value()) {
    return tl::make_unexpected("No input tupleSet to split");
  }

  // split
  const auto &expTupleSets = SplitKernel::split2(*inputTupleSet_, consumerVec_.size());
  if (!expTupleSets.has_value()) {
    return tl::make_unexpected(expTupleSets.error());
  }

  // send
  send(expTupleSets.value());
  sentResult_ = true;
  inputTupleSet_ = nullopt;
  return {};
}

void SplitPOp::send(const vector<shared_ptr<TupleSet>> &tupleSets) {
  for (uint i = 0; i < consumerVec_.size(); ++i) {
    const auto &consumer = consumerVec_[i];
    const auto &tupleSet = tupleSets[i];
    shared_ptr<Message> tupleSetMessage = make_shared<TupleSetMessage>(tupleSet, name());
    ctx()->send(tupleSetMessage, consumer);
  }
}

void SplitPOp::clear() {
  inputTupleSet_ = nullopt;
}

}
