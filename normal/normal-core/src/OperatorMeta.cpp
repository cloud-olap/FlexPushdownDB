//
// Created by matt on 3/3/20.
//

#include "normal/core/OperatorMeta.h"

#include <utility>

namespace normal::core {

OperatorMeta::OperatorMeta(std::string name, caf::actor_id actorId) : name_(std::move(name)), actorId_(actorId) {}

const std::string &OperatorMeta::name() const {
  return name_;
}

const caf::actor_id &OperatorMeta::actorId() const {
  return actorId_;
}

}
