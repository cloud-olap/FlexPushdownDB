//
// Created by Matt Youill on 31/12/19.
//

#include "normal/core/OperatorActor.h"

#include <utility>
#include <normal/core/message/CompleteMessage.h>

#include "normal/core/Globals.h"
#include "normal/core/message/Envelope.h"

namespace normal::core {

OperatorActor::OperatorActor(caf::actor_config &cfg, std::shared_ptr<Operator> opBehaviour) :
    caf::event_based_actor(cfg),
    opBehaviour_(std::move(opBehaviour)) {
  
  this->opBehaviour_->ctx()->operatorActor(this);
}

caf::behavior behaviour(OperatorActor *self) {

  auto functionName = __FUNCTION__;

  return {
      [=](const normal::core::message::Envelope &msg) {

		auto start = std::chrono::steady_clock::now();
        
#define __FUNCTION__ functionName

//        SPDLOG_DEBUG("Message received  |  recipient: '{}', sender: '{}', type: '{}'",
//                     self->operator_()->name(),
//                     msg.message().sender(),
//                     msg.message().type());

        if (msg.message().type() == "CompleteMessage") {
          auto completeMessage = dynamic_cast<const message::CompleteMessage &>(msg.message());
          self->operator_()->ctx()->operatorMap().setComplete(msg.message().sender());
        }

        self->operator_()->onReceive(msg);

		auto finish = std::chrono::steady_clock::now();
		auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
		self->incrementProcessingTime(elapsedTime);
      }
  };
}

caf::behavior OperatorActor::make_behavior() {
  return behaviour(this);
}

std::shared_ptr<normal::core::Operator> OperatorActor::operator_() const {
  return opBehaviour_;
}

long OperatorActor::getProcessingTime() const {
  return processingTime_;
}

void OperatorActor::incrementProcessingTime(long time) {
  processingTime_ += time;
}

}
