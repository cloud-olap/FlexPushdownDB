//
// Created by Yifei Yang on 1/21/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ERRORMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ERRORMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <caf/all.hpp>

namespace fpdb::executor::message {

/**
* Message sent when an exception is captured in actors
*/
class ErrorMessage : public Message {

public:
  explicit ErrorMessage(const std::string &content, const std::string &sender);
  ErrorMessage() = default;
  ErrorMessage(const ErrorMessage&) = default;
  ErrorMessage& operator=(const ErrorMessage&) = default;

  std::string getTypeString() const override;

  const std::string &getContent() const;

private:
  std::string content_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ErrorMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("content", msg.content_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ERRORMESSAGE_H
