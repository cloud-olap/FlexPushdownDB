//
// Created by matt on 9/12/19.
//

#include <fpdb/executor/message/Message.h>
#include <utility>

namespace fpdb::executor::message {

Message::Message(MessageType type, std::string sender) :
  type_(std::move(type)),
  sender_(std::move(sender)) {}

MessageType Message::type() const {
  return type_;
}

const std::string& Message::sender() const {
  return sender_;
}

} // namespace
