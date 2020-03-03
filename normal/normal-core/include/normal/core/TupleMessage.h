//
// Created by matt on 11/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLEMESSAGE_H
#define NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLEMESSAGE_H

#include <memory>

#include "Message.h"
#include "TupleSet.h"

class TupleMessage : public normal::core::Message {
private:
  std::shared_ptr<TupleSet> m_tupleSet;

public:
  explicit TupleMessage(std::shared_ptr<TupleSet> tupleSet);
  ~TupleMessage() override = default;

  std::shared_ptr<TupleSet> data();
};

#endif //NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLEMESSAGE_H
