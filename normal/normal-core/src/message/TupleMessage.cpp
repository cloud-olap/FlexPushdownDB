//
// Created by matt on 11/12/19.
//

#include "normal/core/message/TupleMessage.h"

#include <utility>

namespace normal::core::message {

TupleMessage::TupleMessage(std::shared_ptr<TupleSet> tuples,
                           std::string sender) :
    Message("TupleMessage", std::move(sender)),
    tuples_(std::move(tuples)) {
}

std::shared_ptr<TupleSet> TupleMessage::tuples() const {
  return tuples_;
}

}