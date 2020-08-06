//
// Created by matt on 17/6/20.
//

#include "normal/pushdown/shuffle/Shuffle.h"

#include <utility>
#include <normal/tuple/TupleSet2.h>
#include <normal/tuple/ColumnBuilder.h>
#include <normal/pushdown/shuffle/ATTIC/Shuffler.h>
#include <normal/pushdown/shuffle/ShuffleKernel2.h>

using namespace normal::pushdown::shuffle;
using namespace normal::tuple;

Shuffle::Shuffle(const std::string &Name, std::string ColumnName) :
	Operator(Name, "Shuffle"), columnName_(std::move(ColumnName)) {}

std::shared_ptr<Shuffle> Shuffle::make(const std::string &Name, std::string ColumnName){
  return std::make_shared<Shuffle>(Name, ColumnName);
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
	throw std::runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void Shuffle::onStart() {
  consumers_ = this->ctx()->operatorMap().get(OperatorRelationshipType::Consumer);
  SPDLOG_DEBUG("Starting '{}'  |  columnName: {}, numConsumers: {}", name(), columnName_, consumers_.size());
}

void Shuffle::onComplete(const CompleteMessage &) {
  if (ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)) {
	ctx()->notifyComplete();
  }
}

void Shuffle::onTuple(const TupleMessage &message) {

  // Get the tuple set
  auto &&tupleSet = TupleSet2::create(message.tuples());
//  if(tupleSet->numRows() == 0){
//	return;
//  }

  // Check there are consumers
  if(consumers_.empty()){
	return;
  }
  // Shuffle the tuple set
  auto expectedShuffledTupleSets = ShuffleKernel2::shuffle(columnName_, consumers_.size(), *tupleSet);
  if(!expectedShuffledTupleSets.has_value()){
    throw std::runtime_error(expectedShuffledTupleSets.error());
  }
  auto shuffledTupleSets = expectedShuffledTupleSets.value();

  // Send the shuffled tuple sets to consumers
  size_t partitionIndex = 0;
  for(const auto& shuffledTupleSet: shuffledTupleSets){
	std::shared_ptr<core::message::Message>
		tupleMessage = std::make_shared<core::message::TupleMessage>(shuffledTupleSet->toTupleSetV1(), name());
	ctx()->send(tupleMessage, consumers_[partitionIndex].name());
	++partitionIndex;
  }
}
