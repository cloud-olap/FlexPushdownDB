//
// Created by matt on 5/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_COMPLETEMESSAGE_H
#define NORMAL_NORMAL_CORE_SRC_COMPLETEMESSAGE_H

#include <vector>

#include <caf/all.hpp>

#include "normal/core/message/Message.h"

namespace normal::core::message {
/**
 * Message fired when an operator completes its work
 */
class CompleteMessage : public Message {

public:
  explicit CompleteMessage(std::string sender);

};

}

#endif //NORMAL_NORMAL_CORE_SRC_COMPLETEMESSAGE_H
