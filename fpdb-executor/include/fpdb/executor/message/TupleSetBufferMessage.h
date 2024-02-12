//
// Created by Yifei Yang on 6/26/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETBUFFERMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETBUFFERMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::tuple;

namespace fpdb::executor::message {

class TupleSetBufferMessage: public Message {
  
public:
  explicit TupleSetBufferMessage(const std::shared_ptr<TupleSet> &tuples,
                                 const std::string &consumer,
                                 const std::string &sender);
  TupleSetBufferMessage() = default;
  TupleSetBufferMessage(const TupleSetBufferMessage&) = default;
  TupleSetBufferMessage& operator=(const TupleSetBufferMessage&) = default;
  ~TupleSetBufferMessage() override = default;

  std::string getTypeString() const override;

  const std::shared_ptr<TupleSet>& tuples() const;
  const std::string &getConsumer() const;

private:
  std::shared_ptr<TupleSet> tuples_;
  std::string consumer_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleSetBufferMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("tuples", msg.tuples_),
                                f.field("consumer", msg.consumer_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETBUFFERMESSAGE_H
