//
// Created by Yifei Yang on 2/29/24.
//

#include <fpdb/executor/physical/exchange/BatchExchangeRRForwardPOp.h>

namespace fpdb::executor::physical::exchange {

BatchExchangeRRForwardPOp::BatchExchangeRRForwardPOp(const string &name,
                                                     const vector<string> &projectColumnNames,
                                                     int nodeId):
  PhysicalOp(name, BATCH_EXCHANGE_RR_FORWARD, projectColumnNames, nodeId) {}

std::string BatchExchangeRRForwardPOp::getTypeString() const {
  return "BatchExchangeRRForwardPOp";
}

void BatchExchangeRRForwardPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
    this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(msg.message());
    this->onTupleSet(tupleSetMessage);
  } else if (msg.message().type() == MessageType::TUPLESET_READY_REMOTE) {
    auto tupleSetReadyRemoteMessage = dynamic_cast<const TupleSetReadyRemoteMessage &>(msg.message());
    this->onTupleSetReadyRemote(tupleSetReadyRemoteMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}", msg.message().getTypeString(), name()));
  }
}

void BatchExchangeRRForwardPOp::produce(const std::shared_ptr<PhysicalOp> &op) {
  PhysicalOp::produce(op);
  consumerVec_.emplace_back(op->name());
}

void BatchExchangeRRForwardPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void BatchExchangeRRForwardPOp::onTupleSet(const TupleSetMessage &msg) {
  // simply forward to the consumer corresponding to "nextConsumerId_"
  std::shared_ptr<Message> forwardMsg = std::make_shared<TupleSetMessage>(msg);
  ctx()->send(forwardMsg, consumerVec_[nextConsumerId_++ % consumerVec_.size()]);
}

void BatchExchangeRRForwardPOp::onTupleSetReadyRemote(const TupleSetReadyRemoteMessage &msg) {
  // simply forward to the consumer corresponding to "nextConsumerId_"
  // "sender_" and "originalConsumer_" of the msg should be preserved which is part of the identifier
  // of the batch to be fetched
  std::shared_ptr<Message> forwardMsg = std::make_shared<TupleSetReadyRemoteMessage>(msg);
  ctx()->send(forwardMsg, consumerVec_[nextConsumerId_++ % consumerVec_.size()]);
}

void BatchExchangeRRForwardPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    ctx()->notifyComplete();
  }
}

void BatchExchangeRRForwardPOp::clear() {
  // Noop
}
  
}
