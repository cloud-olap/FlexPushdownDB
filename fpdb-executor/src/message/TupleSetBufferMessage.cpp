//
// Created by Yifei Yang on 6/26/22.
//

#include <fpdb/executor/message/TupleSetBufferMessage.h>

namespace fpdb::executor::message {

TupleSetBufferMessage::TupleSetBufferMessage(const std::shared_ptr<TupleSet> &tuples,
                                             const std::string &consumer,
                                             const std::string &sender) :
  Message(TUPLESET_BUFFER, sender),
  tuples_(tuples),
  consumer_(consumer) {}

std::string TupleSetBufferMessage::getTypeString() const {
  return "TupleSetBufferMessage";
}

const std::shared_ptr<TupleSet>& TupleSetBufferMessage::tuples() const {
  return tuples_;
}

const std::string &TupleSetBufferMessage::getConsumer() const {
  return consumer_;
}
  
}
