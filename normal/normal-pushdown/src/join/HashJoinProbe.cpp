//
// Created by matt on 29/4/20.
//

#include "normal/pushdown/join/HashJoinProbe.h"

#include <utility>
#include <normal/pushdown/join/HashTableMessage.h>

using namespace normal::pushdown::join;

HashJoinProbe::HashJoinProbe(const std::string &name, JoinPredicate pred) :
	Operator(name, "HashJoinProbe"),
	pred_(std::move(pred)) {}

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

}

void HashJoinProbe::onComplete(normal::core::message::CompleteMessage msg) {

}

void HashJoinProbe::onHashTable(HashTableMessage msg) {

}
