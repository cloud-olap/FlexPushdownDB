//
// Created by matt on 3/3/20.
//

#include "normal/core/OperatorMeta.h"

#include <utility>

namespace normal::core {

OperatorMeta::OperatorMeta(std::string name, caf::actor actorHandle) : name_(std::move(name)), actorHandle_(std::move(actorHandle)) {}

const std::string &OperatorMeta::name() const {
  return name_;
}

const caf::actor &OperatorMeta::actorHandle() const {
  return actorHandle_;
}

}
