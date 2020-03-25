//
// Created by matt on 5/3/20.
//

#include "normal/core/CompleteMessage.h"

#include <utility>

namespace normal::core {

CompleteMessage::CompleteMessage(std::string sender) : Message("CompleteMessage", std::move(sender)) {}

}