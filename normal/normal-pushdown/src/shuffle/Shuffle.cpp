//
// Created by matt on 17/6/20.
//

#include "normal/pushdown/shuffle/Shuffle.h"

#include <utility>
#include <normal/tuple/TupleSet2.h>
#include <normal/tuple/ColumnBuilder.h>
#include <normal/pushdown/shuffle/ATTIC/Shuffler.h>
#include <normal/pushdown/shuffle/ShuffleKernel2.h>
#include <normal/pushdown/Globals.h>

using namespace normal::pushdown::shuffle;
using namespace normal::tuple;

Shuffle::Shuffle(const std::string &Name, std::string ColumnName, long queryId) :
	Operator(Name, "Shuffle", queryId), columnName_(std::move(ColumnName)) {}

std::shared_ptr<Shuffle> Shuffle::make(const std::string &Name, const std::string &ColumnName, long queryId) {
  return std::make_shared<Shuffle>(Name, ColumnName, queryId);
}

void Shuffle::onReceive(const Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}" + msg.message().type(), name()));
  }
}

void Shuffle::produce(const std::shared_ptr<Operator> &operator_) {
  Operator::produce(operator_);
  consumers_.emplace_back(operator_->name());
}

void Shuffle::onStart() {
  SPDLOG_DEBUG("Starting '{}'  |  columnName: {}, numConsumers: {}", name(), columnName_, consumers_.size());
  buffers_.resize(consumers_.size(), std::nullopt);
}

void Shuffle::onComplete(const CompleteMessage &) {
  if (ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer) && !hasProcessedAllComplete_) {
    size_t partitionIndex = 0;
    for (const auto &buffer: buffers_) {
      auto sendResult = send(partitionIndex, true);
      if (!sendResult)
        throw std::runtime_error(sendResult.error());
      ++partitionIndex;
    }

    ctx()->notifyComplete();
	  hasProcessedAllComplete_ = true;

	  double shuffleSpeed = (((double) bytesShuffled_) / 1024.0 / 1024.0) / (((double) shuffleTime_) / 1000000000);
//	  SPDLOG_INFO("Shuffle time: {}, numBytes: {}, speed: {}MB/s, numRows: {}, {}", shuffleTime_, bytesShuffled_, shuffleSpeed, numRowShuffled_, name());
  }
}

tl::expected<void, std::string> Shuffle::buffer(const std::shared_ptr<TupleSet2> &tupleSet, int partitionIndex) {

  // Add the tuple set to the buffer
  if (!buffers_[partitionIndex].has_value()) {
	buffers_[partitionIndex] = tupleSet;
  } else {
    const auto &bufferedTupleSet = buffers_[partitionIndex].value();
	const auto &concatenateResult = TupleSet2::concatenate({bufferedTupleSet, tupleSet});
	if (!concatenateResult)
	  return tl::make_unexpected(concatenateResult.error());
	buffers_[partitionIndex] = concatenateResult.value();

	std::shared_ptr<arrow::Table> combinedTable;
	auto status = buffers_[partitionIndex].value()->getArrowTable().value()
		->CombineChunks(arrow::default_memory_pool(), &combinedTable);
	if (!status.ok())
	  return tl::make_unexpected(status.message());
	buffers_[partitionIndex] = TupleSet2::make(combinedTable);
  }

  return {};
}

tl::expected<void, std::string> Shuffle::send(int partitionIndex, bool force) {

  // If the tupleset is big enough, send it, then clear the buffer
  if (force
	  || (buffers_[partitionIndex].has_value() && buffers_[partitionIndex].value()->numRows() >= DefaultBufferSize)) {
	std::shared_ptr<core::message::Message>
		tupleMessage =
		std::make_shared<core::message::TupleMessage>(buffers_[partitionIndex].value()->toTupleSetV1(), name());
	ctx()->send(tupleMessage, consumers_[partitionIndex]);
	buffers_[partitionIndex] = std::nullopt;
  }

  return {};
}

void Shuffle::onTuple(const TupleMessage &message) {

  // Get the tuple set
  const auto &tupleSet = TupleSet2::create(message.tuples());
  std::vector<std::shared_ptr<TupleSet2>> shuffledTupleSets;
  auto startTime = std::chrono::steady_clock::now();

  // Check empty
  if(tupleSet->numRows() == 0){
    for (size_t s = 0; s < consumers_.size(); ++s) {
      shuffledTupleSets.emplace_back(TupleSet2::make(tupleSet->schema().value()));
    }
  }

  else {
    // Shuffle the tuple set
    auto expectedShuffledTupleSets = ShuffleKernel2::shuffle(columnName_, consumers_.size(), *tupleSet);
    if (!expectedShuffledTupleSets.has_value()) {
      throw std::runtime_error(fmt::format("{}, {}", expectedShuffledTupleSets.error(), name()));
    }
    shuffledTupleSets = expectedShuffledTupleSets.value();
  }

  auto stopTime = std::chrono::steady_clock::now();
  auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
  numRowShuffled_ += tupleSet->numRows();
  bytesShuffled_ += tupleSet->size();
  shuffleTime_ += time;

  // Send the shuffled tuple sets to consumers
  size_t partitionIndex = 0;
  for (const auto &shuffledTupleSet: shuffledTupleSets) {
	auto bufferAndSendResult = buffer(shuffledTupleSet, partitionIndex)
		.and_then([&]() { return send(partitionIndex, false); });
	if (!bufferAndSendResult)
	  throw std::runtime_error(bufferAndSendResult.error());
//	std::shared_ptr<core::message::Message>
//		tupleMessage = std::make_shared<core::message::TupleMessage>(shuffledTupleSet->toTupleSetV1(), name());
//	ctx()->send(tupleMessage, consumers_[partitionIndex]);
	++partitionIndex;
  }

}
