//
// Created by matt on 11/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLEMESSAGE_H
#define NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLEMESSAGE_H

#include <memory>

#include <caf/all.hpp>

#include "Message.h"
#include "TupleSet.h"

namespace normal::core {

class TupleMessage : public normal::core::Message {
private:
  std::shared_ptr<normal::core::TupleSet> m_tupleSet;

public:
  explicit TupleMessage(std::shared_ptr<normal::core::TupleSet> tupleSet);
  ~TupleMessage() override = default;

  std::shared_ptr<normal::core::TupleSet> data();
};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::TupleMessage)

#endif //NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLEMESSAGE_H
