//
// Created by matt on 11/12/19.
//

#include "normal/core/TupleMessage.h"

namespace normal::core {

TupleMessage::TupleMessage(std::shared_ptr<normal::core::TupleSet> tupleSet) :
    normal::core::Message("TupleMessage"),
    m_tupleSet(std::move(tupleSet)) {
}

std::shared_ptr<normal::core::TupleSet> TupleMessage::data() {
  return m_tupleSet;
}

}