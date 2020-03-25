//
// Created by matt on 5/1/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H
#define NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H

#include <vector>

#include <caf/all.hpp>

#include "normal/core/Message.h"

namespace normal::core {

/**
 * Message sent to operators to tell them to start doing their "thing"
 */
class StartMessage : public Message {

private:
  std::vector<caf::actor> consumers_;

public:
  explicit StartMessage(std::vector<caf::actor> consumers, std::string from);
  [[nodiscard]] const std::vector<caf::actor> &consumers() const;

};

}

#endif //NORMAL_NORMAL_CORE_SRC_KERNEL_STARTMESSAGE_H
