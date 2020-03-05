//
// Created by matt on 5/1/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H
#define NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H

#include <vector>

#include <caf/all.hpp>

#include "normal/core/Message.h"


class StartMessage : public normal::core::Message {

private:
  std::vector<caf::actor> consumers;

public:
  explicit StartMessage(std::vector<caf::actor> Consumers);
  [[nodiscard]] const std::vector<caf::actor> &getConsumers() const;

};

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(StartMessage)

#endif //NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H
