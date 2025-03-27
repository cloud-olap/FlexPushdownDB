//
// Created by matt on 17/6/20.
//

#include <fpdb/executor/physical/shuffle/ShufflePOp.h>
#include <fpdb/executor/physical/shuffle/ShuffleKernel.h>
#include <fpdb/executor/physical/shuffle/ShuffleKernel2.h>
#include <fpdb/executor/physical/split/SplitKernel.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/ColumnBuilder.h>
#include <utility>

using namespace fpdb::executor::physical::shuffle;
using namespace fpdb::tuple;

ShufflePOp::ShufflePOp(string name,
                       vector<string> projectColumnNames,
                       int nodeId,
                       vector<string> shuffleColumnNames) :
	PhysicalOp(move(name), SHUFFLE, move(projectColumnNames), nodeId),
	shuffleColumnNames_(move(shuffleColumnNames)) {}

ShufflePOp::ShufflePOp(string name,
                       vector<string> projectColumnNames,
                       int nodeId,
                       vector<string> shuffleColumnNames,
                       vector<string> consumerVec) :
  PhysicalOp(move(name), SHUFFLE, move(projectColumnNames), nodeId),
  shuffleColumnNames_(move(shuffleColumnNames)),
  consumerVec_(consumerVec) {}

std::string ShufflePOp::getTypeString() const {
  return "ShufflePOp";
}

void ShufflePOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
	  this->onStart();
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

const std::vector<std::string> &ShufflePOp::getShuffleColumnNames() const {
  return shuffleColumnNames_;
}

const std::vector<std::string> &ShufflePOp::getConsumerVec() const {
  return consumerVec_;
}

void ShufflePOp::setConsumerVec(const std::vector<std::string> &consumerVec) {
  consumerVec_ = consumerVec;
}

void ShufflePOp::addToConsumerVec(const std::shared_ptr<PhysicalOp> &op) {
  consumerVec_.emplace_back(op->name());
}

void ShufflePOp::clearConsumerVec() {
  consumerVec_.clear();
}

void ShufflePOp::produce(const shared_ptr<PhysicalOp> &operator_) {
  PhysicalOp::produce(operator_);
  consumerVec_.emplace_back(operator_->name());
}

void ShufflePOp::enableDistBatchExchange(const std::string &batchExchange,
                                         const vector<string> &batchExchangeConsumers) {
  if (consumers_.find(batchExchange) == consumers_.end()) {
    throw std::runtime_error("\"batchExchange\" not in the consumer set yet");
  }
  isDistPreShuffle_ = true;
  batchExchange_ = batchExchange;
  consumerVec_ = batchExchangeConsumers;
}

void ShufflePOp::produceAddiConsumerVec(const vector<shared_ptr<PhysicalOp>> &addiConsumerOps) {
  if (addiConsumerOps.size() != consumerVec_.size()) {
    throw std::runtime_error(fmt::format("sizes of additional consumers and original consumers mismatch, "
                                         "should be '{}', but got '{}'", consumerVec_.size(), addiConsumerOps.size()));
  }
  vector<string> addiConsumerVec;
  for (const auto &op: addiConsumerOps) {
    PhysicalOp::produce(op);
    addiConsumerVec.emplace_back(op->name());
  }
  addiConsumerVecs_.emplace_back(addiConsumerVec);
}

void ShufflePOp::onStart() {
  SPDLOG_DEBUG("Starting '{}'  |  numConsumers: {}", name(), consumerVec_.size());
  buffers_.resize(consumerVec_.size(), nullopt);
}

void ShufflePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    for (int partitionIndex = 0; partitionIndex < static_cast<int>(buffers_.size()); ++partitionIndex) {
      auto sendResult = send(partitionIndex, true);
      if (!sendResult)
        ctx()->notifyError(sendResult.error());
    }
    ctx()->notifyComplete();
  }
}

tl::expected<void, string> ShufflePOp::buffer(const shared_ptr<TupleSet> &tupleSet, int partitionIndex) {
  // Add the tuple set to the buffer
  if (!buffers_[partitionIndex].has_value()) {
	buffers_[partitionIndex] = tupleSet;
  } else {
    const auto &bufferedTupleSet = buffers_[partitionIndex].value();
	const auto &concatenateResult = TupleSet::concatenate({bufferedTupleSet, tupleSet});
	if (!concatenateResult)
	  return tl::make_unexpected(concatenateResult.error());
	buffers_[partitionIndex] = concatenateResult.value();

	auto expectedTable = buffers_[partitionIndex].value()->table()
		->CombineChunks(arrow::default_memory_pool());
	if (expectedTable.ok())
	  buffers_[partitionIndex] = TupleSet::make(*expectedTable);
	else
	  return tl::make_unexpected(expectedTable.status().message());
  }

  return {};
}

tl::expected<void, string> ShufflePOp::send(int partitionIndex, bool force) {
  // If the tupleset is big enough, send it, then clear the buffer
  if (buffers_[partitionIndex].has_value() &&
      (force || buffers_[partitionIndex].value()->numRows() >= DefaultBufferSize)) {
    auto tupleSet = buffers_[partitionIndex].value();
    auto consumer = consumerVec_[partitionIndex];

    if (!isSeparated_) {
      // If at compute side, do it regularly (send tupleSet to the consumer)
      if (isDistPreShuffle_) {
        // when batch exchange is enabled
        shared_ptr<Message> tupleSetBufferMessage = make_shared<TupleSetBufferMessage>(
                tupleSet, consumerVec_[partitionIndex], name_);
        ctx()->send(tupleSetBufferMessage, batchExchange_);
      } else {
        // otherwise, do it regularly
        shared_ptr<Message> tupleSetMessage = make_shared<TupleSetMessage>(tupleSet, name_);
        ctx()->send(tupleSetMessage, consumer);
      }
      // Send to additional consumers if any
      for (uint i = 0; i < addiConsumerVecs_.size(); ++i) {
        shared_ptr<Message> tupleSetMessage = make_shared<TupleSetMessage>(tupleSet, name_);
        ctx()->send(tupleSetMessage, addiConsumerVecs_[i][partitionIndex]);
      }
    } else {
      // If at storage side, send tupleSet to the root to buffer it
      shared_ptr<Message> tupleSetBufferMessage = make_shared<TupleSetBufferMessage>(tupleSet, consumer, name_);
      ctx()->notifyRoot(tupleSetBufferMessage, consumer);
    }
    buffers_[partitionIndex] = nullopt;
  }

  return {};
}

void ShufflePOp::onTupleSet(const TupleSetMessage &message) {
  // Get the tuple set
  const auto &tupleSet = message.tuples();
  vector<shared_ptr<TupleSet>> shuffledTupleSets;

  // Check empty
  if (tupleSet->numRows() == 0){
    for (size_t s = 0; s < consumerVec_.size(); ++s) {
      shuffledTupleSets.emplace_back(TupleSet::make(tupleSet->schema()));
    }
  }

  else {
    // Shuffle the tuple set, need to handle the case when overflow is happening,
    // i.e., byte size of input `tupleSet` is larger than int32 max
    auto expNoOverflowTupleSets = split::SplitKernel::splitForOverFlow(tupleSet);
    if (!expNoOverflowTupleSets.has_value()) {
      ctx()->notifyError(expNoOverflowTupleSets.error());
      return;
    }
    const auto &noOverflowTupleSets = *expNoOverflowTupleSets;
    if (noOverflowTupleSets.size() == 1) {
      auto expShuffledTupleSets = shuffle(noOverflowTupleSets[0]);
      if (!expShuffledTupleSets.has_value()) {
        ctx()->notifyError(expShuffledTupleSets.error());
        return;
      }
      shuffledTupleSets = *expShuffledTupleSets;
    } else {
      std::vector<std::vector<std::shared_ptr<TupleSet>>> shuffledTupleSetPieces;
      shuffledTupleSetPieces.resize(consumerVec_.size());
      shuffledTupleSets.resize(consumerVec_.size());
      for (const auto &noOverflowTupleSet: noOverflowTupleSets) {
        auto expShuffledTupleSets = shuffle(noOverflowTupleSet);
        if (!expShuffledTupleSets.has_value()) {
          ctx()->notifyError(expShuffledTupleSets.error());
          return;
        }
        for (size_t i = 0; i < consumerVec_.size(); ++i) {
          shuffledTupleSetPieces[i].emplace_back((*expShuffledTupleSets)[i]);
        }
      }
      for (size_t i = 0; i < consumerVec_.size(); ++i) {
        auto expConcatTupleSet = TupleSet::concatenate(shuffledTupleSetPieces[i]);
        if (!expConcatTupleSet.has_value()) {
          ctx()->notifyError(expConcatTupleSet.error());
          return;
        }
        shuffledTupleSets[i] = *expConcatTupleSet;
      }
    }
  }

  // Send the shuffled tuple sets to consumers
  int partitionIndex = 0;
  for (const auto &shuffledTupleSet: shuffledTupleSets) {
    auto bufferAndSendResult = buffer(shuffledTupleSet, partitionIndex)
      .and_then([&]() { return send(partitionIndex, false); });
    if (!bufferAndSendResult)
      ctx()->notifyError(bufferAndSendResult.error());
    ++partitionIndex;
  }
}

tl::expected<std::vector<std::shared_ptr<TupleSet>>, std::string>
ShufflePOp::shuffle(const std::shared_ptr<TupleSet> &tupleSet) {
  return USE_SHUFFLE_KERNEL_2 ?
          ShuffleKernel2::shuffle(shuffleColumnNames_, consumerVec_.size(), tupleSet) :
          ShuffleKernel::shuffle(shuffleColumnNames_, consumerVec_.size(), *tupleSet);
}

void ShufflePOp::clear() {
  buffers_.clear();
}
