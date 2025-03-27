//
// Created by Yifei Yang on 5/9/24.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterSplitPOp.h>
#include <fpdb/executor/flight/FlightHandler.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterSplitPOp::BloomFilterSplitPOp(const std::string &name,
                                         const std::vector<std::string> &projectColumnNames,
                                         int nodeId):
  PhysicalOp(name, BLOOM_FILTER_SPLIT, projectColumnNames, nodeId) {}

void BloomFilterSplitPOp::onReceive(const Envelope &envelope) {
  const auto &msg = envelope.message();

  if (msg.type() == MessageType::START) {
    this->onStart();
  } else if (msg.type() == MessageType::BLOOM_FILTER) {
    auto bloomFilterMessage = dynamic_cast<const BloomFilterMessage &>(msg);
    this->onBloomFilter(bloomFilterMessage);
  } else if (msg.type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg);
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + msg.getTypeString());
  }
}

std::string BloomFilterSplitPOp::getTypeString() const {
  return "BloomFilterSplitPOp";
}

void BloomFilterSplitPOp::produce(const std::shared_ptr<PhysicalOp> &op) {
  PhysicalOp::produce(op);
  consumerVec_.emplace_back(op->name());
}

void BloomFilterSplitPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void BloomFilterSplitPOp::onBloomFilter(const BloomFilterMessage &msg) {
  const auto &bloomFilter = msg.getBloomFilter();
  if (bloomFilter->getType() != BloomFilterType::GLOBAL_ARROW_BF) {
    ctx()->notifyError("Currently only global Arrow BF is supported when splitting bloom filter");
    return;
  }

  // split
  std::vector<std::shared_ptr<GlobalArrowBloomFilter>> splitRes;
  if (consumerVec_.empty()) {
    ctx()->notifyError("No consumers when splitting bloom filter");
    return;
  }
  if (consumerVec_.size() == 1) {
    splitRes.resize(1);
    splitRes[0] = std::static_pointer_cast<GlobalArrowBloomFilter>(bloomFilter);
  } else {
    auto expSplitRes = std::static_pointer_cast<GlobalArrowBloomFilter>(bloomFilter)->split(consumerVec_.size());
    if (!expSplitRes.has_value()) {
      ctx()->notifyError(expSplitRes.error());
      return;
    }
    splitRes = *expSplitRes;
  }

  // send
  for (uint i = 0; i < consumerVec_.size(); ++i) {
    // skip empty split if needed
    if (splitRes[i] == nullptr) {
      continue;
    }
    // get consumer node id
    auto opEntry = ctx()->operatorMap().get(consumerVec_[i]);
    if (!opEntry.has_value()) {
      ctx()->notifyError(opEntry.error());
      return;
    }
    // check remote consumers when sending
    auto bloomFilterMessage = std::make_shared<BloomFilterMessage>(splitRes[i], name_);
    if ((*opEntry).getNodeId() != nodeId_) {
      flight::FlightHandler::daemonServer_->putBitmap(queryId_, name_, consumerVec_[i], splitRes[i]);
      bloomFilterMessage->setRemoteInfo({flight::FlightHandler::daemonServer_->getHost(),
                                         flight::FlightHandler::daemonServer_->getPort()});
      bloomFilterMessage->setRemoteConsumerSpecific(true);
    }
    ctx()->send(bloomFilterMessage, consumerVec_[i]);
  }
}

void BloomFilterSplitPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    ctx()->notifyComplete();
  }
}

void BloomFilterSplitPOp::clear() {
  // noop
}

}
