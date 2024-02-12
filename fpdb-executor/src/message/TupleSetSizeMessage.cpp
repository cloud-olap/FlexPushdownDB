//
// Created by Yifei Yang on 3/18/22.
//

#include <fpdb/executor/message/TupleSetSizeMessage.h>

namespace fpdb::executor::message {

TupleSetSizeMessage::TupleSetSizeMessage(int64_t numRows, const std::string &sender) :
  Message(TUPLESET_SIZE, sender),
  numRows_(numRows) {
}

std::string TupleSetSizeMessage::getTypeString() const {
  return "TupleSetSizeMessage";
}

int64_t TupleSetSizeMessage::getNumRows() const {
  return numRows_;
}
  
}
