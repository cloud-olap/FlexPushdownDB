//
// Created by Yifei Yang on 5/9/24.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterConcatPOp.h>
#include <fpdb/executor/physical/bloomfilter/GlobalArrowBloomFilter.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterConcatPOp::BloomFilterConcatPOp(const std::string &name,
                                           const std::vector<std::string> &projectColumnNames,
                                           int nodeId):
  PhysicalOp(name, BLOOM_FILTER_CONCAT, projectColumnNames, nodeId) {}

void BloomFilterConcatPOp::consume(const std::shared_ptr<PhysicalOp> &op) {
  PhysicalOp::consume(op);
  uint id = producerIds_.size();
  producerIds_[op->name()] = id;
}

void BloomFilterConcatPOp::onReceive(const Envelope &envelope) {
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

std::string BloomFilterConcatPOp::getTypeString() const {
  return "BloomFilterConcatPOp";
}

void BloomFilterConcatPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void BloomFilterConcatPOp::onBloomFilter(const BloomFilterMessage &msg) {
  if (bloomFilterParts_.empty()) {
    bloomFilterParts_.resize(producerIds_.size());
  }
  const auto &bloomFilter = msg.getBloomFilter();
  const auto &remoteInfo = msg.getRemoteInfo();

  // if the bloom filter is from another node
  if (remoteInfo.has_value()) {
    readRemoteBloomFilter(bloomFilter.get(), msg.sender(), *remoteInfo, msg.isRemoteConsumerSpecific());
  }

  // check and buffer
  auto it = producerIds_.find(msg.sender());
  if (it == producerIds_.end()) {
    ctx()->notifyError(fmt::format("Unknown producer when concatenating bloom filter parts: '{}'", msg.sender()));
    return;
  }
  bloomFilterParts_[it->second] = bloomFilter;
}

void BloomFilterConcatPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    // concat bloom filter parts
    std::vector<std::shared_ptr<GlobalArrowBloomFilter>> typedBloomFilterParts;
    for (uint i = 0; i < bloomFilterParts_.size(); ++i) {
      if (bloomFilterParts_[i] == nullptr) {
        // this denotes empty split, we just skip
        continue;
      }
      if (bloomFilterParts_[i]->getType() != BloomFilterType::GLOBAL_ARROW_BF) {
        ctx()->notifyError("Currently only global Arrow BF is supported when concatenating bloom filter parts");
        return;
      }
      typedBloomFilterParts.emplace_back(std::static_pointer_cast<GlobalArrowBloomFilter>(bloomFilterParts_[i]));
    }
    auto expConcatedBf = GlobalArrowBloomFilter::concat(typedBloomFilterParts);
    if (!expConcatedBf.has_value()) {
      ctx()->notifyError(expConcatedBf.error());
      return;
    }

    // send bloom filter to consumers, here we only have local consumers
    auto bloomFilterMessage = std::make_shared<BloomFilterMessage>(*expConcatedBf, name_);
    ctx()->tell(bloomFilterMessage);

    ctx()->notifyComplete();
  }
}

void BloomFilterConcatPOp::clear() {
  bloomFilterParts_.clear();
}

}
