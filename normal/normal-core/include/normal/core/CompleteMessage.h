//
// Created by matt on 5/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_COMPLETEMESSAGE_H
#define NORMAL_NORMAL_CORE_SRC_COMPLETEMESSAGE_H

#include <vector>

#include <caf/all.hpp>

#include "Message.h"

class CompleteMessage : public normal::core::Message {
public:
  CompleteMessage();
};

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(CompleteMessage)

#endif //NORMAL_NORMAL_CORE_SRC_COMPLETEMESSAGE_H
