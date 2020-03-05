//
// Created by Matt Youill on 31/12/19.
//

#include "normal/core/OperatorActor.h"

#include "normal/core/Globals.h"
#include <normal/core/Envelope.h>

#include <utility>

OperatorActor::OperatorActor(caf::actor_config &cfg, std::shared_ptr<normal::core::Operator> _operator) :
    caf::event_based_actor(cfg), _operator(std::move(_operator)) {
  std::shared_ptr Ptr = this->_operator;
  Ptr->ctx()->setOperatorActor(this);
}

caf::behavior behaviour(OperatorActor *self) {
  return {
      [=](const normal::core::Envelope &msg) {
        SPDLOG_DEBUG("Message received  |  actor: '{}', messageKind: '{}'",
                     self->_operator->name(),
                     msg.message().type());
        self->_operator->onReceive(msg);
      }
  };
}

caf::behavior OperatorActor::make_behavior() {
  return behaviour(this);
}

