//
// Created by matt on 5/1/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_STARTMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_STARTMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <caf/all.hpp>
#include <vector>

namespace fpdb::executor::message {

/**
 * Message sent to operators to tell them to start doing their "thing"
 */
class StartMessage : public Message {

public:
  explicit StartMessage(std::string from);
  StartMessage() = default;
  StartMessage(const StartMessage&) = default;
  StartMessage& operator=(const StartMessage&) = default;

  std::string getTypeString() const override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, StartMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_STARTMESSAGE_H
