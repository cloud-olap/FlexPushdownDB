//
// Created by matt on 9/12/19.
//

#include "normal/core/Message.h"

#include <utility>

namespace normal::core {

Message::Message(std::string type, std::string from) :
    type_(std::move(type)),
    from_(std::move(from)) {}

Message::~Message() = default;

std::string Message::type() const {
  return type_;
}
std::string Message::from() const {
  return from_;
}

} // namespace
