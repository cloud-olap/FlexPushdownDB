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

//  this->opBehaviour_->ctx()->operatorActor(this);
}

caf::behavior behaviour(OperatorActor *self) {

  auto ctx = self->operator_()->weakCtx().lock();
  if(!ctx)
	throw std::runtime_error("Could not get operator context  |  Could not acquire shared pointer from weak pointer");
  ctx->operatorActor(self);

  auto functionName = __FUNCTION__;

  return {
      [=](const normal::core::message::Envelope &msg) {

		auto start = std::chrono::steady_clock::now();
        
 //#define __FUNCTION__ functionName

//        SPDLOG_DEBUG("Message received  |  recipient: '{}', sender: '{}', type: '{}'",
//                     self->operator_()->name(),
//                     msg.message().sender(),
//                     msg.message().type());

		if (msg.message().type() == "CompleteMessage") {
		  auto completeMessage = dynamic_cast<const message::CompleteMessage &>(msg.message());
		  self->operator_()->ctx()->operatorMap().setComplete(msg.message().sender());
		}
		else if (msg.message().type() == "StartMessage") {
		  self->running_ = true;
		}
		else if (msg.message().type() == "StopMessage") {
		  self->running_ = false;
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

void OperatorActor::on_exit() {
  SPDLOG_DEBUG("Stopping operator  |  name: '{}'", this->opBehaviour_->name());

  /*
   * Need to delete the actor handle in operator otherwise CAF will never release the actor
   *
   * Since all the pointers the actors maintain with each other refer to this handle, only need
   * to destroy this one and all the cycles are broken
   */
  this->opBehaviour_->destroyActor();
}

}
