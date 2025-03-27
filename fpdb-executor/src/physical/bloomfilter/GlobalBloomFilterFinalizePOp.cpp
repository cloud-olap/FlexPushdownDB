//
// Created by Yifei Yang on 4/1/24.
//

#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterFinalizePOp.h>
#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterInitPOp.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/flight/FlightHandler.h>

using namespace fpdb::store::server::flight;

namespace fpdb::executor::physical::bloomfilter {

GlobalBloomFilterFinalizePOp::GlobalBloomFilterFinalizePOp(const std::string &name,
                                                           const std::vector<std::string> &projectColumnNames,
                                                           int nodeId):
  PhysicalOp(name, GLOBAL_BLOOM_FILTER_FINALIZE, projectColumnNames, nodeId) {}

void GlobalBloomFilterFinalizePOp::connectToProducers(
        const std::shared_ptr<PhysicalOp> &bfFinalize,
        const std::shared_ptr<PhysicalOp> &bfInit,
        const std::vector<std::shared_ptr<PhysicalOp>> &bfCreateVec) {
  std::static_pointer_cast<bloomfilter::GlobalBloomFilterInitPOp>(bfInit)->produceFinalize(bfFinalize);
  bfFinalize->consume(bfInit);
  for (const auto &bfCreate: bfCreateVec) {
    bfCreate->PhysicalOp::produce(bfFinalize);    // need to call the overridden one
    bfFinalize->consume(bfCreate);
  }
}

void GlobalBloomFilterFinalizePOp::connectToConsumers(
        const std::shared_ptr<PhysicalOp> &bfFinalize,
        const std::vector<std::shared_ptr<PhysicalOp>> &localBloomFilterReceivers,
        const std::vector<std::shared_ptr<PhysicalOp>> &remoteBloomFilterReceivers) {
  for (const auto &localBloomFilterReceiver: localBloomFilterReceivers) {
    std::static_pointer_cast<GlobalBloomFilterFinalizePOp>(bfFinalize)
            ->addLocalBloomFilterReceiver(localBloomFilterReceiver);
    localBloomFilterReceiver->consume(bfFinalize);
  }
  for (const auto &remoteBloomFilterReceiver: remoteBloomFilterReceivers) {
    std::static_pointer_cast<GlobalBloomFilterFinalizePOp>(bfFinalize)
            ->addRemoteBloomFilterReceiver(remoteBloomFilterReceiver);
    remoteBloomFilterReceiver->consume(bfFinalize);
  }
}

void GlobalBloomFilterFinalizePOp::onReceive(const Envelope &envelope) {
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

std::string GlobalBloomFilterFinalizePOp::getTypeString() const {
  return "GlobalBloomFilterFinalizePOp";
}

void GlobalBloomFilterFinalizePOp::addLocalBloomFilterReceiver(const std::shared_ptr<PhysicalOp> &op) {
  localBloomFilterReceivers_.emplace(op->name());
  PhysicalOp::produce(op);
}

void GlobalBloomFilterFinalizePOp::addRemoteBloomFilterReceiver(const std::shared_ptr<PhysicalOp> &op) {
  remoteBloomFilterReceivers_.emplace(op->name());
  PhysicalOp::produce(op);
}

void GlobalBloomFilterFinalizePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void GlobalBloomFilterFinalizePOp::onBloomFilter(const BloomFilterMessage &msg) {
  if (bloomFilter_ != nullptr) {
    ctx()->notifyError("Duplicate bloom filters received by GlobalBloomFilterFinalizePOp");
  }
  bloomFilter_ = msg.getBloomFilter();
}

void GlobalBloomFilterFinalizePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    // check bloom filter first, except for "DistGlobalBloomFilterMerge" with "USE_PARALLEL_DIST_GLOBAL_BF_MERGE"
    if (bloomFilter_ == nullptr &&
       !(type_ == POpType::DIST_GLOBAL_BLOOM_FILTER_MERGE && USE_PARALLEL_DIST_GLOBAL_BF_MERGE)) {
      ctx()->notifyError("No bloom filter received.");
      return;
    }

    // send bloom filter if any
    if (bloomFilter_ != nullptr) {
      // send to local receivers
      auto bloomFilterMessage = std::make_shared<BloomFilterMessage>(bloomFilter_, name_);
      if (!localBloomFilterReceivers_.empty()) {
        ctx()->tell(bloomFilterMessage, localBloomFilterReceivers_);
      }

      // send to remote receivers
      if (!remoteBloomFilterReceivers_.empty()) {
        flight::FlightHandler::daemonServer_->putBitmap(queryId_, name_,
                                                        bloomFilter_, remoteBloomFilterReceivers_.size());
        // need to make a new copy of message instead of using the one sent to local receivers
        bloomFilterMessage = std::make_shared<BloomFilterMessage>(bloomFilter_, name_);
        bloomFilterMessage->setRemoteInfo({flight::FlightHandler::daemonServer_->getHost(),
                                           flight::FlightHandler::daemonServer_->getPort()});
        ctx()->tell(bloomFilterMessage, remoteBloomFilterReceivers_);
      }
    }

    ctx()->notifyComplete();
  }
}

void GlobalBloomFilterFinalizePOp::clear() {
  bloomFilter_.reset();
}

}
