//
// Created by matt on 5/3/20.
//

#include "normal/core/message/CompleteMessage.h"

#include <utility>

namespace normal::core::message {

CompleteMessage::CompleteMessage(std::string sender) : Message("CompleteMessage", std::move(sender)) {}

}