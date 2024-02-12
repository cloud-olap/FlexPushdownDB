//
// Created by matt on 9/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_MESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_MESSAGE_H

#include <fpdb/executor/message/MessageType.h>
#include <string>

namespace fpdb::executor::message {

/**
 * Base class for messages
 */
class Message {

public:
  explicit Message(MessageType type, std::string sender);
  Message() = default;
  Message(const Message&) = default;
  Message& operator=(const Message&) = default;
  virtual ~Message() = default;

  MessageType type() const;
  const std::string& sender() const;
  virtual std::string getTypeString() const = 0;

protected:
  MessageType type_;
  std::string sender_;

};

} // namespace

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_MESSAGE_H
