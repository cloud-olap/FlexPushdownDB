//
// Created by Matt Youill on 31/12/19.
//

#include "normal/core/OperatorActor.h"

#include <utility>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/message/ConnectMessage.h>

#include "normal/core/Globals.h"
#include "normal/core/message/Envelope.h"

namespace normal::core {

OperatorActor::OperatorActor(caf::actor_config &cfg, std::shared_ptr<Operator> opBehaviour) :
	caf::event_based_actor(cfg),
	opBehaviour_(std::move(opBehaviour)) {

  name_ = opBehaviour_->name();
}

caf::behavior behaviour(OperatorActor *self) {

  auto ctx = self->operator_()->weakCtx();
  if (!ctx)
	throw std::runtime_error("Could not get operator context  |  Could not acquire shared pointer from weak pointer");
  ctx->operatorActor(self);

  auto functionName = __FUNCTION__;

  return {
	  [=](GetProcessingTimeAtom) {
		auto start = std::chrono::steady_clock::now();
		auto finish = std::chrono::steady_clock::now();
		auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
		self->incrementProcessingTime(elapsedTime);
		return self->getProcessingTime();
	  },
	  [=](const normal::core::message::Envelope &msg) {

		auto start = std::chrono::steady_clock::now();

		//#define __FUNCTION__ functionName

    SPDLOG_DEBUG("Message received  |  recipient: '{}', sender: '{}', type: '{}'",
                 self->operator_()->name(),
                 msg.message().sender(),
                 msg.message().type());

		if (msg.message().type() == "ConnectMessage") {
		  auto connectMessage = dynamic_cast<const message::ConnectMessage &>(msg.message());

		  for (const auto &element: connectMessage.connections()) {
			auto localEntry = LocalOperatorDirectoryEntry(element.getName(),
														  element.getActorHandle(),
														  element.getConnectionType(),
														  false);

			self->operator_()->weakCtx()->operatorMap().insert(localEntry);
		  }
		}
		else if (msg.message().type() == "StartMessage") {
		  auto startMessage = dynamic_cast<const message::StartMessage &>(msg.message());

		  self->running_ = true;

		  self->operator_()->onReceive(msg);

		  while(!self->buffer_.empty()){
			auto item = self->buffer_.front();
			self->overriddenMessageSender_ = item.second;
			self->call_handler(self->current_behavior(), item.first);
			self->buffer_.pop();
		  }

		  self->overriddenMessageSender_ = std::nullopt;

		} else if (msg.message().type() == "StopMessage") {
		  self->running_ = false;

		  self->operator_()->onReceive(msg);
		}
		else{
		  if(!self->running_){
			self->buffer_.emplace(self->current_mailbox_element()->move_content_to_message(), self->current_sender());
		  }
		  else {
			if (msg.message().type() == "CompleteMessage") {
			  auto completeMessage = dynamic_cast<const message::CompleteMessage &>(msg.message());
			  self->operator_()->weakCtx()->operatorMap().setComplete(msg.message().sender());
			}

			self->operator_()->onReceive(msg);
		  }
		}

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
   */
  this->opBehaviour_->destroyActor();
}

}
