//
// Created by matt on 5/1/20.
//

#include "StartMessage.h"

#include <utility>

StartMessage::StartMessage(std::vector<caf::actor_id> consumers) : consumers(std::move(consumers)) {}

const std::vector<caf::actor_id> &StartMessage::getConsumers() const {
  return consumers;
}
