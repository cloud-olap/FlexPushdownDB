//
// Created by Yifei Yang on 4/17/24.
//

#include <fpdb/executor/physical/bloomfilter/DistGlobalBloomFilterInitPOp.h>

namespace fpdb::executor::physical::bloomfilter {

DistGlobalBloomFilterInitPOp::DistGlobalBloomFilterInitPOp(const string &name,
                                                           const vector<string> &projectColumnNames,
                                                           int nodeId):
  PhysicalOp(name, DIST_GLOBAL_BLOOM_FILTER_INIT, projectColumnNames, nodeId) {}

std::string DistGlobalBloomFilterInitPOp::getTypeString() const {
  return "DistGlobalBloomFilterInitPOp";
}

void DistGlobalBloomFilterInitPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
    this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET_SIZE) {
    auto tupleSetSizeMessage = dynamic_cast<const TupleSetSizeMessage &>(msg.message());
    this->onTupleSetSize(tupleSetSizeMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}", msg.message().getTypeString(), name()));
  }
}

void DistGlobalBloomFilterInitPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void DistGlobalBloomFilterInitPOp::onTupleSetSize(const TupleSetSizeMessage &msg) {
  numRows_ += msg.getNumRows();
}

void DistGlobalBloomFilterInitPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    auto masks = std::make_shared<arrow::compute::BloomFilterMasks>(arrow::compute::BlockedBloomFilter::global_masks_);
    auto distGlobalBFInitMessage = std::make_shared<DistGlobalArrowBFInitMessage>(numRows_, masks, name_);
    ctx()->tell(distGlobalBFInitMessage);
    ctx()->notifyComplete();
  }
}

void DistGlobalBloomFilterInitPOp::clear() {
  // noop
}

}
