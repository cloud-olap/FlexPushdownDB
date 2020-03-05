//
// Created by matt on 11/12/19.
//

#include "normal/core/TupleMessage.h"

#include <algorithm>

TupleMessage::TupleMessage(std::shared_ptr<TupleSet> tupleSet) :
    normal::core::Message("TupleMessage"),
    m_tupleSet(std::move(tupleSet)) {
}

std::shared_ptr<TupleSet> TupleMessage::data() {
  return m_tupleSet;
}
