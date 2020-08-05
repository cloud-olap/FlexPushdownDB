//
// Created by matt on 4/8/20.
//

#include "normal/pushdown/join/TupleSetIndexMessage.h"

#include <utility>

using namespace normal::pushdown;
using namespace normal::pushdown::join;

TupleSetIndexMessage::TupleSetIndexMessage(std::shared_ptr<TupleSetIndex> tupleSetIndex, const std::string &sender) :
	Message("TupleSetIndexMessage", sender),
	tupleSetIndex_(std::move(tupleSetIndex)) {
}

const std::shared_ptr<TupleSetIndex> &TupleSetIndexMessage::getTupleSetIndex() const {
  return tupleSetIndex_;
}