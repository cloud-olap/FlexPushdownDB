//
// Created by matt on 11/12/19.
//

#include <fpdb/executor/message/TupleSetMessage.h>
#include <utility>

namespace fpdb::executor::message {

TupleSetMessage::TupleSetMessage(std::shared_ptr<TupleSet> tuples,
                           std::string sender) :
  Message(TUPLESET, std::move(sender)),
  tuples_(std::move(tuples)) {
}

std::string TupleSetMessage::getTypeString() const {
  return "TupleSetMessage";
}

const std::shared_ptr<TupleSet>& TupleSetMessage::tuples() const {
  return tuples_;
}

}