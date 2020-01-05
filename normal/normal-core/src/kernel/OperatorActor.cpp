//
// Created by Matt Youill on 31/12/19.
//

#include <spdlog/spdlog.h>
#include "OperatorActor.h"

OperatorActor::OperatorActor(caf::actor_config &cfg, const std::shared_ptr<Operator> &_operator) :
    OperatorActorType::base(cfg) {
  this->_operator = _operator;
}

// function-based, statically typed, event-based API
OperatorActor::behavior_type typed_calculator_fun(OperatorActor &self) {
  return {
      [&](const StartMessage &msg) {
        spdlog::info("Actor Received");
        self._operator->receive(nullptr);
      }
  };
}

OperatorActor::behavior_type OperatorActor::make_behavior() {
  return typed_calculator_fun(*this);
}
