//
// Created by Yifei Yang on 10/24/23.
//

#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterInitPOp.h>
#include <fpdb/executor/physical/split/SplitKernel.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/message/GlobalArrowBloomFilterInitMessage.h>
#include <fpdb/tuple/util/Util.h>

namespace fpdb::executor::physical::bloomfilter {

GlobalBloomFilterInitPOp::GlobalBloomFilterInitPOp(const std::string &name,
                                                   const std::vector<std::string> &projectColumns,
                                                   int nodeId,
                                                   const std::vector<std::string> &bloomFilterColumns,
                                                   int numDistSubBf):
  PhysicalOp(name, GLOBAL_BLOOM_FILTER_INIT, projectColumns, nodeId),
  bloomFilterColumns_(bloomFilterColumns),
  numDistSubBf_(numDistSubBf) {}

void GlobalBloomFilterInitPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
    this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(msg.message());
    this->onTupleSet(tupleSetMessage);
  } else if (msg.message().type() == MessageType::DIST_GLOBAL_BF_INIT) {
    distInitMsg_ = dynamic_cast<const DistGlobalArrowBFInitMessage &>(msg.message());
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}", msg.message().getTypeString(), name()));
  }
}

std::string GlobalBloomFilterInitPOp::getTypeString() const {
  return "GlobalBloomFilterInitPOp";
}

void GlobalBloomFilterInitPOp::produceFinalize(const std::shared_ptr<PhysicalOp> &op) {
  if (!bfFinalizeOp_.empty()) {
    throw std::runtime_error("Duplicated GlobalBloomFilterFinalizePOp set");
  }
  bfFinalizeOp_ = op->name();
  PhysicalOp::produce(op);
}

void GlobalBloomFilterInitPOp::recordPredTransCard(const executor::cache::PredTransCardCache::PredTransCardKey &key) {
  ptCardInfo_.collect_ = true;
  ptCardInfo_.key_ = key;
}

void GlobalBloomFilterInitPOp::clear() {
  input_.clear();
}

void GlobalBloomFilterInitPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void GlobalBloomFilterInitPOp::onTupleSet(const TupleSetMessage &message) {
  input_.emplace_back(message.tuples());
  numRows_ += message.tuples()->numRows();
  if (!keyLen_.has_value()) {
    keyLen_ = tuple::util::Util::getFixLen(message.tuples()->schema());
  }
}

void GlobalBloomFilterInitPOp::onComplete(const CompleteMessage &) {
  // do only when all producers complete
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    // check input not empty
    if (input_.empty()) {
      ctx()->notifyError("No input received");
      return;
    }

    // concatenate input tables into a single one
    auto expConcatenatedTupleSet = TupleSet::concatenate(input_);
    if (!expConcatenatedTupleSet.has_value()) {
      ctx()->notifyError(expConcatenatedTupleSet.error());
      return;
    }
    auto concatenatedTupleSet = *expConcatenatedTupleSet;

    // split input and send
    auto expTupleSets = split::SplitKernel::split2(concatenatedTupleSet,
                                                   consumers_.size() - 1 /*exclude "GlobalBloomFilterInitFinalize"*/);
    if (!expTupleSets.has_value()) {
      ctx()->notifyError(expTupleSets.error());
      return;
    }
    auto tupleSets = *expTupleSets;
    uint i = 0;
    for (const auto &consumer: consumers_) {
      // exclude "GlobalBloomFilterInitFinalize"
      if (consumer == bfFinalizeOp_) {
        continue;
      }
      auto tupleSetMessage = std::make_shared<TupleSetMessage>(tupleSets[i++], name_);
      ctx()->send(tupleSetMessage, consumer);
    }

    // create and init global bloom filter, then broadcast it to all create ops
    if (USE_ARROW_BLOOM_FILTER_IMPL) {
      auto globalArrowBloomFilter = std::make_shared<GlobalArrowBloomFilter>(
              distInitMsg_.has_value() ? (*distInitMsg_).getNumRows() : numRows_, /*use numRows in the entire cluster if any*/
              bloomFilterColumns_,
              consumers_.size() - 1 /*exclude "GlobalBloomFilterInitFinalize"*/, numDistSubBf_);
      globalArrowBloomFilter->init(distInitMsg_.has_value() ? (*distInitMsg_).getMasks() : nullptr);  /*use received masks if any*/
      i = 0;
      for (const auto &consumer: consumers_) {
        // exclude "GlobalBloomFilterInitFinalize" first since thread id matters
        if (consumer == bfFinalizeOp_) {
          continue;
        }
        auto globalArrowBloomFilterInitMessage =
                std::make_shared<GlobalArrowBloomFilterInitMessage>(i++, globalArrowBloomFilter, name_);
        ctx()->send(globalArrowBloomFilterInitMessage, consumer);
      }
      // send to "GlobalBloomFilterInitFinalize" separately
      auto bloomFilterMessage = std::make_shared<BloomFilterMessage>(globalArrowBloomFilter, name_);
      ctx()->send(bloomFilterMessage, bfFinalizeOp_);
#if SHOW_DEBUG_METRICS == true
      // send PT case study metrics
      if (ptCSMetricsInfo_.collPredTransCSMetrics_) {
        if (!distInitMsg_.has_value() || nodeId_ == 0 /* count only one for dist global BF */) {
          metrics::PredTransCSMetrics::PTCSMetricsUnit ptCSMetricsUnit(ptCSMetricsInfo_);
          ptCSMetricsUnit.bfSize_ = globalArrowBloomFilter->getBlockedBloomFilter()->num_blocks() * sizeof(uint64_t);
          std::shared_ptr<Message> ptCSMetricsMsg = std::make_shared<PredTransCSMetricsMessage>(
                  ptCSMetricsUnit, name_);
          ctx()->notifyRoot(ptCSMetricsMsg);
        }
      }
#endif
    } else {
      // currently not supported
      ctx()->notifyError("Global Vanilla Bloom filter is currently not supported");
      return;
    }

    // record cardinality if needed
    sendPTCardMessage(ptCardInfo_, numRows_, keyLen_);

    // complete
    ctx()->notifyComplete();
  }
}

}
