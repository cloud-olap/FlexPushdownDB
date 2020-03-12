//
// Created by matt on 5/1/20.
//

#include "normal/core/StartMessage.h"

#include <utility>

namespace normal::core {

StartMessage::StartMessage(std::vector<caf::actor> consumers) :
    normal::core::Message("StartMessage"),
    consumers_(std::move(consumers)) {}

const std::vector<caf::actor> &StartMessage::consumers() const {
  return consumers_;
}

}
