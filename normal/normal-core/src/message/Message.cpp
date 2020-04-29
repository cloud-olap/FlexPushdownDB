//
// Created by matt on 9/12/19.
//

#include "normal/core/message/Message.h"

#include <utility>

namespace normal::core::message {

Message::Message(std::string type, std::string sender) :
    type_(std::move(type)),
    sender_(std::move(sender)) {}

std::string Message::type() const {
  return type_;
}

std::string Message::sender() const {
  return sender_;
}

} // namespace
