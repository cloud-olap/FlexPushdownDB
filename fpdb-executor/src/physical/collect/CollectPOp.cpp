//
// Created by Yifei Yang on 10/13/22.
//

#include <fpdb/executor/physical/collect/CollectPOp.h>

namespace fpdb::executor::physical::collect {

CollectPOp::CollectPOp(const string &name,
                       const vector<string> &projectColumnNames,
                       int nodeId):
  PhysicalOp(name, COLLECT, projectColumnNames, nodeId) {}

std::string CollectPOp::getTypeString() const {
  return "CollectPOp";
}

void CollectPOp::onReceive(const Envelope &msg) {
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

void CollectPOp::produce(const std::shared_ptr<PhysicalOp> &op) {
  if (!consumers_.empty()) {
    throw std::runtime_error("CollectPOp should only have one consumer");
  }
  PhysicalOp::produce(op);
}

void CollectPOp::consume(const std::shared_ptr<PhysicalOp> &op) {
  orderedProducers_.emplace_back(op->name());
  PhysicalOp::consume(op);
}

const std::vector<std::string> &CollectPOp::getOrderedProducers() const {
  return orderedProducers_;
}

void CollectPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void CollectPOp::onTupleSet(const TupleSetMessage &message) {
  std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(message.tuples(), name_);
  ctx()->tell(tupleSetMessage);
}

void CollectPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    ctx()->notifyComplete();
  }
}

void CollectPOp::clear() {
  // Noop
}
  
}
