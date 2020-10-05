//
// Created by matt on 5/1/20.
//

#include "normal/core/message/StartMessage.h"

#include <utility>

namespace normal::core::message {

StartMessage::StartMessage(
                           std::string from) :
    Message("StartMessage", std::move(from)){

}

}
