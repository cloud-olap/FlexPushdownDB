//
// Created by Yifei Yang on 4/5/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ADAPTRESUMEMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ADAPTRESUMEMESSAGE_H

#include <fpdb/executor/message/Message.h>

namespace fpdb::executor::message {
/**
 * Message to continue the next stage of adaptive exec, i.e., let "AdaptSinkPOp" forward results of the previous stage
 */
class AdaptResumeMessage: public Message {

public:
  AdaptResumeMessage(bool preserve, std::string sender);
  AdaptResumeMessage() = default;
  AdaptResumeMessage(const AdaptResumeMessage&) = default;
  AdaptResumeMessage& operator=(const AdaptResumeMessage&) = default;

  std::string getTypeString() const override;

  bool preserve() const;

private:
  bool preserve_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, AdaptResumeMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("preserve", msg.preserve_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ADAPTRESUMEMESSAGE_H
