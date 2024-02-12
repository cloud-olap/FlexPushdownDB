//
// Created by matt on 4/8/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETINDEXMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETINDEXMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/tuple/TupleSetIndex.h>
#include <memory>

using namespace fpdb::tuple;

namespace fpdb::executor::message {

class TupleSetIndexMessage : public Message {

public:
  TupleSetIndexMessage(std::shared_ptr<TupleSetIndex> tupleSetIndex, const std::string &sender);
  TupleSetIndexMessage() = default;
  TupleSetIndexMessage(const TupleSetIndexMessage&) = default;
  TupleSetIndexMessage& operator=(const TupleSetIndexMessage&) = default;

  std::string getTypeString() const override;

  [[nodiscard]] const std::shared_ptr<TupleSetIndex> &getTupleSetIndex() const;

private:
  std::shared_ptr<TupleSetIndex> tupleSetIndex_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleSetIndexMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("tuples", msg.tupleSetIndex_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETINDEXMESSAGE_H
