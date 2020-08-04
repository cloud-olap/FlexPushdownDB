//
// Created by matt on 4/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_TUPLESETINDEXMESSAGE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_TUPLESETINDEXMESSAGE_H

#include <memory>

#include <normal/core/message/Message.h>
#include "TupleSetIndex.h"

using namespace normal::core::message;

namespace normal::pushdown::join {

class TupleSetIndexMessage : public Message {

public:
  TupleSetIndexMessage(std::shared_ptr<TupleSetIndex> tupleSetIndex, const std::string &sender);

  [[nodiscard]] const std::shared_ptr<TupleSetIndex> &getTupleSetIndex() const;

private:
  std::shared_ptr<TupleSetIndex> tupleSetIndex_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_TUPLESETINDEXMESSAGE_H
