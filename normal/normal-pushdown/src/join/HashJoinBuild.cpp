//
// Created by matt on 29/4/20.
//

#include "normal/pushdown/join/HashJoinBuild.h"

#include <utility>



using namespace normal::pushdown::join;

HashJoinBuild::HashJoinBuild(const std::string &name, std::string columnName) :
	Operator(name, "HashJoinBuild"),
	columnName_(std::move(columnName)) {}

void HashJoinBuild::onReceive(const normal::core::message::Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void HashJoinBuild::onStart() {

}

void HashJoinBuild::onTuple(normal::core::message::TupleMessage msg) {

}

void HashJoinBuild::onComplete(normal::core::message::CompleteMessage msg) {

}

