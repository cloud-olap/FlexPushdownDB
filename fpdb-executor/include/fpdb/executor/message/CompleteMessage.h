//
// Created by matt on 5/3/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_COMPLETEMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_COMPLETEMESSAGE_H

#include <fpdb/executor/message/Message.h>

namespace fpdb::executor::message {
/**
 * Message fired when an operator completes its work
 */
class CompleteMessage : public Message {

public:
  explicit CompleteMessage(std::string sender);
  CompleteMessage() = default;
  CompleteMessage(const CompleteMessage&) = default;
  CompleteMessage& operator=(const CompleteMessage&) = default;

  std::string getTypeString() const override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CompleteMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_COMPLETEMESSAGE_H
