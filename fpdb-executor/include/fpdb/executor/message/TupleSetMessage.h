//
// Created by matt on 11/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/tuple/TupleSet.h>
#include <memory>

using namespace fpdb::tuple;

namespace fpdb::executor::message {

/**
 * Message containing a list of tuples
 */
class TupleSetMessage : public Message {

public:
  explicit TupleSetMessage(std::shared_ptr<TupleSet> tuples, std::string sender);
  TupleSetMessage() = default;
  TupleSetMessage(const TupleSetMessage&) = default;
  TupleSetMessage& operator=(const TupleSetMessage&) = default;
  ~TupleSetMessage() override = default;

  std::string getTypeString() const override;

  const std::shared_ptr<TupleSet>& tuples() const;

private:
  std::shared_ptr<TupleSet> tuples_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleSetMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("tuples", msg.tuples_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETMESSAGE_H
