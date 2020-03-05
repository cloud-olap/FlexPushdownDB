//
// Created by matt on 5/1/20.
//

#include "normal/core/StartMessage.h"

#include <utility>

StartMessage::StartMessage(std::vector<caf::actor> consumers) :
    normal::core::Message("StartMessage"),
    consumers(std::move(consumers)) {}

const std::vector<caf::actor> &StartMessage::getConsumers() const {
  return consumers;
}
