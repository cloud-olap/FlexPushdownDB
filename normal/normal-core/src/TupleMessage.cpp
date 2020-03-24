//
// Created by matt on 11/12/19.
//

#include "normal/core/TupleMessage.h"

namespace normal::core {

TupleMessage::TupleMessage(std::shared_ptr<normal::core::TupleSet> tuples, std::string from) :
    normal::core::Message("TupleMessage", from),
    tuples_(std::move(tuples)) {
}

std::shared_ptr<normal::core::TupleSet> TupleMessage::tuples() const {
  return tuples_;
}

}