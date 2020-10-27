//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorContext.h"

#include <utility>
#include <cassert>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/SegmentCacheActor.h>
#include "normal/core/Globals.h"
#include "normal/core/message/Message.h"
#include "normal/core/Actors.h"

namespace normal::core {

void OperatorContext::tell(std::shared_ptr<message::Message> &msg) {

  assert(this);

  if(complete_)
	throw std::runtime_error(fmt::format("Cannot tell message to consumers, operator {} ('{}') is complete", this->operatorActor()->id(), this->operatorActor()->operator_()->name()));

  message::Envelope e(msg);

  // Send message to consumers
  for(const auto& consumer: this->operatorActor()->operator_()->consumers()){
    caf::actor actorHandle = operatorMap_.get(consumer.first).value().getActor();
    operatorActor_->anon_send(actorHandle, e);
  }
}

tl::expected<void, std::string> OperatorContext::send(const std::shared_ptr<message::Message> &msg, const std::string& recipientId) {

  message::Envelope e(msg);

  if(recipientId == "SegmentCache"){
    if(msg->type() == "LoadRequestMessage"){
      operatorActor_->request(segmentCacheActor_, infinite, normal::core::cache::LoadAtom::value, std::static_pointer_cast<normal::core::cache::LoadRequestMessage>(msg))
      .then([=](const std::shared_ptr<normal::core::cache::LoadResponseMessage>& response){
      operatorActor_->anon_send(this->operatorActor(), Envelope(response));
//		  send(response, this->operator_->name());
      });
    }
    else if(msg->type() == "StoreRequestMessage"){
      operatorActor_->anon_send(segmentCacheActor_, normal::core::cache::StoreAtom::value, std::static_pointer_cast<normal::core::cache::StoreRequestMessage>(msg));
    }
    else if(msg->type() == "WeightRequestMessage"){
      operatorActor_->anon_send(segmentCacheActor_, normal::core::cache::WeightAtom::value, std::static_pointer_cast<normal::core::cache::WeightRequestMessage>(msg));
    }
    else{
      throw std::runtime_error("Unrecognized message " + msg->type());
    }

	return {};
  }

  auto expectedOperator = operatorMap_.get(recipientId);
  if(expectedOperator.has_value()){
    auto recipientOperator = expectedOperator.value();
	operatorActor_->anon_send(recipientOperator.getActor(), e);
	return {};
  }
  else{
  	return tl::unexpected(fmt::format("Actor with id '{}' not found", recipientId));
  }
}

OperatorContext::OperatorContext(caf::actor rootActor, caf::actor segmentCacheActor):
    operatorActor_(nullptr),
    rootActor_(std::move(rootActor)),
    segmentCacheActor_(std::move(segmentCacheActor))
{}

LocalOperatorDirectory &OperatorContext::operatorMap() {
  return operatorMap_;
}
OperatorActor* OperatorContext::operatorActor() {
  return operatorActor_;
}
void OperatorContext::operatorActor(OperatorActor *operatorActor) {
  operatorActor_ = operatorActor;
}

/**
 * Sends a CompleteMessage to all consumers and the root actor
 */
void OperatorContext::notifyComplete() {

  SPDLOG_DEBUG("Completing operator  |  source: {} ('{}')", this->operatorActor()->id(), this->operatorActor()->operator_()->name());
  if(complete_)
    throw std::runtime_error(fmt::format("Cannot complete already completed operator {} ('{}')", this->operatorActor()->id(), this->operatorActor()->operator_()->name()));

  OperatorActor* operatorActor = this->operatorActor();

  std::shared_ptr<message::Message> msg = std::make_shared<message::CompleteMessage>(operatorActor->operator_()->name());
  message::Envelope e(msg);

  // Send message to consumers
  for(const auto& consumer: this->operatorActor()->operator_()->consumers()){
    caf::actor actorHandle = operatorMap_.get(consumer.first).value().getActor();
    operatorActor->anon_send(actorHandle, e);
  }

  // Send message to root actor
  operatorActor->anon_send(rootActor_, e);

  complete_ = true;

//  operatorActor->quit();
}

bool OperatorContext::isComplete() const {
  return complete_;
}

void OperatorContext::destroyActorHandles() {
  operatorMap_.destroyActorHandles();
  destroy(rootActor_);
  destroy(segmentCacheActor_);
}

} // namespace
