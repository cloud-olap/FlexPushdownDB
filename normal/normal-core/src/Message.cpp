//
// Created by matt on 9/12/19.
//

#include "normal/core/Message.h"

#include <utility>

namespace normal::core {

Message::~Message() = default;

//Message::Message(std::string type) : type_(std::move(type)) {}


std::string Message::type() const {
  return type_;
}

} // namespace
