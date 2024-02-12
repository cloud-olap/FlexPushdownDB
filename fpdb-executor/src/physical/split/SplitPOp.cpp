//
// Created by Yifei Yang on 12/13/21.
//

#include <fpdb/executor/physical/split/SplitPOp.h>
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
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}" + msg.message().getTypeString(), name()));
  }
}

void SplitPOp::onStart() {
  SPDLOG_DEBUG("Starting '{}'  |  numConsumers: {}", name(), consumerVec_.size());
}

void SplitPOp::onTupleSet(const TupleSetMessage &message) {
  // get tupleSet
  const auto &tupleSet = message.tuples();
  if (tupleSet->numRows() == 0) {
    return;
  }

  // buffer
  const auto &result = bufferInput(tupleSet);
  if (!result.has_value()) {
    ctx()->notifyError(result.error());
  }

  // send if buffer is large enough
  if (inputTupleSet_.has_value() && inputTupleSet_.value()->numRows() >= DefaultBufferSize * (int) consumerVec_.size()) {
    splitAndSend();
  }
}

void SplitPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    if (inputTupleSet_.has_value() && inputTupleSet_.value()->numRows() > 0) {
      splitAndSend();
    }

    ctx()->notifyComplete();
  }
}

void SplitPOp::produce(const shared_ptr<PhysicalOp> &op) {
  PhysicalOp::produce(op);
  consumerVec_.emplace_back(op->name());
}

tl::expected<void, string> SplitPOp::bufferInput(const shared_ptr<TupleSet>& tupleSet) {
  if (!inputTupleSet_.has_value()) {
    inputTupleSet_ = tupleSet;
    return {};
  }
  const auto &result = inputTupleSet_.value()->append(tupleSet);
  if (!result.has_value()) {
    return result;
  }
  return {};
}

arrow::ArrayVector splitArray(const shared_ptr<arrow::Array> &array, uint n) {
  int64_t splitSize = array->length() / n;
  arrow::ArrayVector splitArrays{n};
  for (uint i = 0; i < n; ++i) {
    int64_t offset = i * splitSize;
    if (i < n - 1) {
      splitArrays[i] = array->Slice(offset, splitSize);
    } else {
      splitArrays[i] = array->Slice(offset);
    }
  }
  return splitArrays;
}

tl::expected<void, string> SplitPOp::splitAndSend() {
  const auto &expTupleSets = split();
  if (!expTupleSets.has_value()) {
    return tl::make_unexpected(expTupleSets.error());
  }
  send(expTupleSets.value());
  clear();
  return {};
}

tl::expected<vector<shared_ptr<TupleSet>>, string> SplitPOp::split() {
  if (!inputTupleSet_.has_value()) {
    return tl::make_unexpected("No input tupleSet to split");
  }

  // combine chunks
  const auto &inputTable = inputTupleSet_.value()->table();
  const auto &expCombinedTable = inputTable->CombineChunks();
  if (!expCombinedTable.ok()) {
    return tl::make_unexpected(expCombinedTable.status().message());
  }
  const auto &combinedTable = expCombinedTable.ValueOrDie();

  // split
  vector<arrow::ArrayVector> outputArrayVectors{consumerVec_.size()};
  for (const auto &column: combinedTable->columns()) {
    const auto &inputArray = column->chunk(0);
    const auto &splitArrays = splitArray(inputArray, consumerVec_.size());
    for (uint i = 0; i < consumerVec_.size(); ++i) {
      outputArrayVectors[i].emplace_back(splitArrays[i]);
    }
  }

  // make tables
  vector<shared_ptr<TupleSet>> outputTupleSets{consumerVec_.size()};
  const auto &schema = inputTupleSet_.value()->schema();
  for (uint i = 0; i < consumerVec_.size(); ++i) {
    outputTupleSets[i] = TupleSet::make(schema, outputArrayVectors[i]);
  }
  return outputTupleSets;
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
