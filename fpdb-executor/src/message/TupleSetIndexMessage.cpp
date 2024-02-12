//
// Created by matt on 4/8/20.
//

#include <fpdb/executor/message/TupleSetIndexMessage.h>
#include <utility>

using namespace fpdb::executor::message;

TupleSetIndexMessage::TupleSetIndexMessage(std::shared_ptr<TupleSetIndex> tupleSetIndex, const std::string &sender) :
	Message(TUPLESET_INDEX, sender),
	tupleSetIndex_(std::move(tupleSetIndex)) {}

std::string TupleSetIndexMessage::getTypeString() const {
  return "TupleSetIndexMessage";
}

const std::shared_ptr<TupleSetIndex> &TupleSetIndexMessage::getTupleSetIndex() const {
  return tupleSetIndex_;
}