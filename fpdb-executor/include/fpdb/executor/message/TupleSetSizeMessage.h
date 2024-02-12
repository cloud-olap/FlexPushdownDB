//
// Created by Yifei Yang on 3/18/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETSIZEMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETSIZEMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <memory>

namespace fpdb::executor::message {

/**
 * Message denoting the size of tupleSet sent to consumers
 */
class TupleSetSizeMessage : public Message {

public:
  explicit TupleSetSizeMessage(int64_t numRows, const std::string &sender);
  TupleSetSizeMessage() = default;
  TupleSetSizeMessage(const TupleSetSizeMessage&) = default;
  TupleSetSizeMessage& operator=(const TupleSetSizeMessage&) = default;
  ~TupleSetSizeMessage() override = default;

  std::string getTypeString() const override;

  int64_t getNumRows() const;

private:
  int64_t numRows_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleSetSizeMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("numRows", msg.numRows_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETSIZEMESSAGE_H
