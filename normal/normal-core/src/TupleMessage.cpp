//
// Created by matt on 11/12/19.
//

#include "normal/core/TupleMessage.h"

#include <utility>

namespace normal::core {

TupleMessage::TupleMessage(std::shared_ptr<normal::core::TupleSet> tuples,
                           std::string sender) :
    Message("TupleMessage", std::move(sender)),
    tuples_(std::move(tuples)) {
}

std::shared_ptr<normal::core::TupleSet> TupleMessage::tuples() const {
  return tuples_;
}

}