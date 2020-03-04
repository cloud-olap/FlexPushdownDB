//
// Created by Matt Youill on 31/12/19.
//

#include "OperatorActor.h"

#include <spdlog/spdlog.h>

#include <normal/core/Envelope.h>

OperatorActor::OperatorActor(caf::actor_config &cfg, const std::shared_ptr<normal::core::Operator> &_operator) :
 caf::event_based_actor(cfg) {
  this->_operator = _operator;
}

caf::behavior behaviour(OperatorActor &self) {
  return {
      [&](const normal::core::Envelope &msg) {
        spdlog::info("Actor Received");
        self._operator->receive(msg);
      }
  };
}

caf::behavior OperatorActor::make_behavior() {
  return behaviour(*this);
}
