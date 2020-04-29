//
// Created by matt on 29/4/20.
//

#include "normal/pushdown/join/HashJoinProbe.h"

#include <utility>
#include <normal/pushdown/join/HashTableMessage.h>

using namespace normal::pushdown::join;

HashJoinProbe::HashJoinProbe(const std::string &name, JoinPredicate pred) :
	Operator(name, "HashJoinProbe"),
	pred_(std::move(pred)),
	hashtable_(std::make_shared<std::unordered_multimap<std::shared_ptr<arrow::Scalar>, long>>()) {
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

}

void HashJoinProbe::onTuple(normal::core::message::TupleMessage msg) {

  // Add the tuples to the internal buffer
  bufferTuples(msg);
}

void HashJoinProbe::bufferTuples(normal::core::message::TupleMessage msg) {
  if (!tuples_) {
	// Initialise tuples buffer with message contents
	tuples_ = msg.tuples();
  } else {
	// Append message contents to tuples buffer
	auto tables = {tuples_->table(), msg.tuples()->table()};
	auto res = arrow::ConcatenateTables(tables);
	if (!res.ok()) {
	  tuples_->table(*res);
	} else {
	  // FIXME: Propagate error properly
	  throw std::runtime_error(res.status().message());
	}
  }
}

void HashJoinProbe::onComplete(normal::core::message::CompleteMessage) {
  if (ctx()->operatorMap().allComplete(normal::core::OperatorRelationshipType::Producer)) {
	joinAndSendTuples();
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

tl::expected<std::shared_ptr<normal::core::TupleSet>, std::string>
HashJoinProbe::join() {
  // TODO: Implement
  return tl::unexpected(std::string("Not implemented yet"));
}

void HashJoinProbe::sendTuples(std::shared_ptr<normal::core::TupleSet> &joined) {
  std::shared_ptr<core::message::Message>
	  tupleMessage = std::make_shared<core::message::TupleMessage>(joined, name());
  ctx()->tell(tupleMessage);
}

void HashJoinProbe::onHashTable(HashTableMessage msg) {
  // Add the hashtable to the internal buffer
  bufferHashTable(msg);
}

void HashJoinProbe::bufferHashTable(HashTableMessage msg) {
  hashtable_->merge(*msg.getHashtable());
}
