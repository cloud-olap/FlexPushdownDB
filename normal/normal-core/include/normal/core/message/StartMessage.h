//
// Created by matt on 5/1/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H
#define NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H

#include <vector>

#include <caf/all.hpp>
#include <normal/core/OperatorConnection.h>

#include "normal/core/message/Message.h"

namespace normal::core::message {

/**
 * Message sent to operators to tell them to start doing their "thing"
 */
class StartMessage : public Message {

public:
  explicit StartMessage(std::string from);

};

}

#endif //NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H
