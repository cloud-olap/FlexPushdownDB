//
// Created by matt on 29/4/20.
//

#include "normal/pushdown/join/HashJoinProbe.h"

#include <utility>

#include <normal/tuple/arrow/SchemaHelper.h>

#include <normal/pushdown/join/HashTableMessage.h>
#include <normal/pushdown/Globals.h>
#include <normal/pushdown/join/Joiner.h>

using namespace normal::pushdown::join;

HashJoinProbe::HashJoinProbe(const std::string &name, JoinPredicate pred) :
	Operator(name, "HashJoinProbe"),
	pred_(std::move(pred)),
	hashtable_(std::make_shared<HashTable>()) {
}

void HashJoinProbe::onReceive(const normal::core::message::Envelope &msg) {

  // FIXME: Really need to get rid of these if type == string tests... Urgh

  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "HashTableMessage") {
	auto hashTableMessage = dynamic_cast<const HashTableMessage &>(msg.message());
	this->onHashTable(hashTableMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void HashJoinProbe::onStart() {
  SPDLOG_DEBUG("Starting  |  Local operator directory:\n{}", ctx()->operatorMap().showString());
}

void HashJoinProbe::onTuple(const normal::core::message::TupleMessage &msg) {

  // Add the tuples to the internal buffer
  bufferTuples(msg);
}

void HashJoinProbe::bufferTuples(const normal::core::message::TupleMessage &msg) {

  auto tupleSet = TupleSet2::create(msg.tuples());

  if (!tuples_) {
	// Initialise tuples buffer with message contents
	tuples_ = tupleSet;
  } else {
	auto result = tuples_->append(tupleSet);
	if(!result.has_value())
	  throw std::runtime_error(result.error());
  }
}

void HashJoinProbe::onComplete(const normal::core::message::CompleteMessage &) {
  if (ctx()->operatorMap().allComplete(normal::core::OperatorRelationshipType::Producer)) {
	joinAndSendTuples();
	ctx()->notifyComplete();
  }
}

void HashJoinProbe::joinAndSendTuples() {
  auto expectedJoinedTuples = join();
  if (expectedJoinedTuples) {
	sendTuples(expectedJoinedTuples.value());
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error(expectedJoinedTuples.error());
  }
}

tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> HashJoinProbe::join() {
  Joiner joiner(pred_, hashtable_, tuples_);
  auto joinedTuplesExpected = joiner.join();
  return joinedTuplesExpected;
}

void HashJoinProbe::sendTuples(const std::shared_ptr<normal::tuple::TupleSet2> &joined) {

  auto v1TupleSet = joined->toTupleSetV1();

  std::shared_ptr<core::message::Message>
	  tupleMessage = std::make_shared<core::message::TupleMessage>(v1TupleSet, name());
  ctx()->tell(tupleMessage);
}

void HashJoinProbe::onHashTable(const HashTableMessage &msg) {
  // Add the hashtable to the internal buffer
  bufferHashTable(msg);
}

void HashJoinProbe::bufferHashTable(const HashTableMessage &msg) {
  hashtable_->merge(msg.getHashtable());
}
