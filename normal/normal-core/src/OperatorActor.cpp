//
// Created by Matt Youill on 31/12/19.
//

#include "normal/core/OperatorActor.h"

#include <utility>

#include "normal/core/Globals.h"
#include "normal/core/Envelope.h"

namespace normal::core {

OperatorActor::OperatorActor(caf::actor_config &cfg, std::shared_ptr<normal::core::Operator> opBehaviour) :
    caf::event_based_actor(cfg),
    opBehaviour_(std::move(opBehaviour)) {

  this->opBehaviour_->ctx()->operatorActor(this);
}

caf::behavior behaviour(OperatorActor *self) {

  auto functionName = __FUNCTION__;

  return {
      [=](const normal::core::Envelope &msg) {

#define __FUNCTION__ functionName

        SPDLOG_DEBUG("Message received  |  actor: '{}', messageKind: '{}'",
                     self->operator_()->name(),
                     msg.message().type());

        self->operator_()->onReceive(msg);
      }
  };
}

caf::behavior OperatorActor::make_behavior() {
  return behaviour(this);
}

std::shared_ptr<normal::core::Operator> OperatorActor::operator_() const {
  return opBehaviour_;
}

}
