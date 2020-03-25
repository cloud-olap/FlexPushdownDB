//
// Created by matt on 9/12/19.
//

#include "normal/core/Message.h"

#include <utility>

namespace normal::core {

Message::Message(std::string type, std::string sender) :
    type_(std::move(type)),
    sender_(std::move(sender)) {}

Message::~Message() = default;

std::string Message::type() const {
  return type_;
}

std::string Message::sender() const {
  return sender_;
}

} // namespace
