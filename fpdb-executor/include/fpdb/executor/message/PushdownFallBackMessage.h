//
// Created by Yifei Yang on 12/15/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PUSHDOWNFALLBACKMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PUSHDOWNFALLBACKMESSAGE_H

#include <fpdb/executor/message/Message.h>

namespace fpdb::executor::message {

/**
 * Message denoting one request falls back to pullup during adaptive execution.
 */
class PushdownFallBackMessage : public Message {

public:
  PushdownFallBackMessage(const std::string &sender);
  PushdownFallBackMessage() = default;
  PushdownFallBackMessage(const PushdownFallBackMessage&) = default;
  PushdownFallBackMessage& operator=(const PushdownFallBackMessage&) = default;
  ~PushdownFallBackMessage() override = default;

  std::string getTypeString() const override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, PushdownFallBackMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PUSHDOWNFALLBACKMESSAGE_H
