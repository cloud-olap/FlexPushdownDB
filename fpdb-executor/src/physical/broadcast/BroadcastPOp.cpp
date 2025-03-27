//
// Created by Yifei Yang on 10/28/23.
//

#include <fpdb/executor/physical/broadcast/BroadcastPOp.h>
#include <fpdb/executor/physical/Globals.h>
namespace fpdb::executor::physical::broadcast {

BroadcastPOp::BroadcastPOp(const string &name,
                           const vector<string> &projectColumnNames,
                           int nodeId,
                           bool enableBatchExchange):
  PhysicalOp(name, BROADCAST, projectColumnNames, nodeId),
  enableBatchExchange_(enableBatchExchange) {}

std::string BroadcastPOp::getTypeString() const {
  return "BroadcastPOp";
}

void BroadcastPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
    this->onStart();
  } else if (msg.message().type() == MessageType::BLOOM_FILTER) {
    auto bloomFilterMessage = dynamic_cast<const BloomFilterMessage &>(msg.message());
    this->onBloomFilter(bloomFilterMessage);
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

void BroadcastPOp::consume(const std::shared_ptr<PhysicalOp> &op) {
  orderedProducers_.emplace_back(op->name());
  PhysicalOp::consume(op);
}

const std::vector<std::string> &BroadcastPOp::getOrderedProducers() const {
  return orderedProducers_;
}

void BroadcastPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
  if (producers_.empty()) {   // this may occur in single-node exec where there is no remote producer
    ctx()->notifyComplete();
  }
}

void BroadcastPOp::onBloomFilter(const BloomFilterMessage &msg) {
  const auto &bloomFilter = msg.getBloomFilter();
  const auto &remoteInfo = msg.getRemoteInfo();

  // if the bloom filter is from another node
  if (remoteInfo.has_value()) {
    readRemoteBloomFilter(bloomFilter.get(), msg.sender(), *remoteInfo, msg.isRemoteConsumerSpecific());
  }

  // broadcast to all consumers
  auto bloomFilterMessage = std::make_shared<BloomFilterMessage>(bloomFilter, name_);
  ctx()->tell(bloomFilterMessage);
}

void BroadcastPOp::onTupleSet(const TupleSetMessage &message) {
  // broadcast tupleSet to all consumers
  const auto &tupleSet = message.tuples();
  if (enableBatchExchange_) {
    if (batchExchangeBuffer_ == nullptr) {
      batchExchangeBuffer_ = tupleSet;
    } else {
      auto expConcatTupleSet = TupleSet::concatenate({batchExchangeBuffer_, tupleSet});
      if (!expConcatTupleSet.has_value()) {
        ctx()->notifyError(expConcatTupleSet.error());
        return;
      }
      batchExchangeBuffer_ = *expConcatTupleSet;
    }
    if (batchExchangeBuffer_->numRows() >= DIST_EXCHANGE_BATCH_SIZE) {
      std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(batchExchangeBuffer_, name_);
      ctx()->tell(tupleSetMessage);
      batchExchangeBuffer_ = nullptr;
    }
  } else {
    std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(tupleSet, name_);
    ctx()->tell(tupleSetMessage);
  }
}

void BroadcastPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    if (batchExchangeBuffer_ != nullptr) {
      std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(batchExchangeBuffer_, name_);
      ctx()->tell(tupleSetMessage);
      batchExchangeBuffer_ = nullptr;
    }

    ctx()->notifyComplete();
  }
}

void BroadcastPOp::clear() {
  // Noop
}
  
}
