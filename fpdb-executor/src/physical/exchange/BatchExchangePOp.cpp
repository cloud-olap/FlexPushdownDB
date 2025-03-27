//
// Created by Yifei Yang on 2/28/24.
//

#include <fpdb/executor/physical/exchange/BatchExchangePOp.h>
#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor::physical::exchange {

BatchExchangePOp::BatchExchangePOp(const string &name,
                                   const vector<string> &projectColumnNames,
                                   int nodeId):
  PhysicalOp(name, BATCH_EXCHANGE, projectColumnNames, nodeId) {}

std::string BatchExchangePOp::getTypeString() const {
  return "BatchExchangePOp";
}

void BatchExchangePOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
    this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET_BUFFER) {
    auto tupleSetBufferMessage = dynamic_cast<const TupleSetBufferMessage &>(msg.message());
    this->onTupleSetBuffer(tupleSetBufferMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}", msg.message().getTypeString(), name()));
  }
}

void BatchExchangePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void BatchExchangePOp::onTupleSetBuffer(const TupleSetBufferMessage &msg) {
  // resize buffer if not yet
  if (buffer_.tables_.empty()) {
    fetchConsumerNodeId();
  }

  // get dst node id
  const auto &table = msg.tuples()->table();
  const auto &consumer = msg.getConsumer();
  auto it = consumerToDstNodeId_.find(consumer);
  if (it == consumerToDstNodeId_.end()) {
    ctx()->notifyError(fmt::format("DstNodeId not found for consumer: '{}'", consumer));
    return;
  }
  int dstNodeId = it->second;

  // buffer table
  if (buffer_.tables_[dstNodeId] == nullptr) {
    buffer_.tables_[dstNodeId] = table;
  } else {
    if (table->num_rows() > 0) {
      auto expConcatenatedTable = arrow::ConcatenateTables({buffer_.tables_[dstNodeId], table});
      if (!expConcatenatedTable.ok()) {
        ctx()->notifyError(expConcatenatedTable.status().message());
        return;
      }
      buffer_.tables_[dstNodeId] = *expConcatenatedTable;
    }
  }

  // send if accumulated enough rows
  if (buffer_.tables_[dstNodeId]->num_rows() >= DIST_EXCHANGE_BATCH_SIZE) {
    send(dstNodeId);
  }
}

void BatchExchangePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    // send if there are remaining rows
    for (int i = 0; i < (int) consumers_.size(); ++i) {
      send(i);
    }

    ctx()->notifyComplete();
  }
}

void BatchExchangePOp::fetchConsumerNodeId() {
  buffer_.tables_.resize(consumers_.size());
  dstNodeIdToConsumer_.resize(consumers_.size());
  for (const auto &consumer: consumers_) {
    auto expOpEntry = ctx()->operatorMap().get(consumer);
    if (!expOpEntry.has_value()) {
      ctx()->notifyError(expOpEntry.error());
      return;
    }
    int dstNodeId = (*expOpEntry).getNodeId();
    dstNodeIdToConsumer_[dstNodeId] = consumer;
    consumerToDstNodeId_[consumer] = dstNodeId;
  }
}

void BatchExchangePOp::send(int dstNodeId) {
  if (buffer_.tables_[dstNodeId] == nullptr) {
    return;
  }
  // send only project columns if not empty
  auto tupleSet = TupleSet::make(buffer_.tables_[dstNodeId]);
  if (!projectColumnNames_.empty()) {
    auto expProjectTupleSet = tupleSet->projectExist(projectColumnNames_);
    if (!expProjectTupleSet.has_value()) {
      ctx()->notifyError(expProjectTupleSet.error());
      return;
    }
    tupleSet = *expProjectTupleSet;
  }
  std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(tupleSet, name_);
  ctx()->send(tupleSetMessage, dstNodeIdToConsumer_[dstNodeId]);
  buffer_.tables_[dstNodeId] = nullptr;
}

void BatchExchangePOp::clear() {
  buffer_.tables_.clear();
}

}
