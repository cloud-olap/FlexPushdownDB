//
// Created by matt on 11/12/19.
//

#include "normal/core/TupleMessage.h"

namespace normal::core {

TupleMessage::TupleMessage(std::shared_ptr<normal::core::TupleSet> tuples) :
    normal::core::Message("TupleMessage"),
    tuples_(std::move(tuples)) {
}

std::shared_ptr<normal::core::TupleSet> TupleMessage::tuples() const {
  return tuples_;
}

}