//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorContext.h"

#include <utility>
#include <cassert>
#include <normal/core/CompleteMessage.h>

#include "normal/core/Globals.h"
#include "normal/core/Message.h"          // for Message
#include "normal/core/OperatorManager.h"  // for OperatorManager
#include "normal/core/Actors.h"  // for OperatorManager

namespace normal::core {

void OperatorContext::tell(std::shared_ptr<normal::core::Message> &msg) {

  assert(this);

  OperatorActor* oa = this->operatorActor();
  normal::core::Envelope e(msg);

  // Send message to consumers
  for(const auto& consumer: this->operator_->consumers()){
    caf::actor actorHandle = consumer.second->actorHandle();
    oa->send(actorHandle, e);
  }
}

OperatorContext::OperatorContext(std::shared_ptr<normal::core::Operator> op,  caf::actor& rootActor):
    operator_(std::move(op)),
    operatorActor_(nullptr),
    rootActor_(rootActor)
{}

std::shared_ptr<normal::core::Operator> OperatorContext::op() {
  return operator_;
}

std::map<std::string, normal::core::OperatorMeta> &OperatorContext::operatorMap() {
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

  OperatorActor* operatorActor = this->operatorActor();

  std::shared_ptr<normal::core::Message> msg = std::make_shared<normal::core::CompleteMessage>(operatorActor->operator_()->name());
  normal::core::Envelope e(msg);

  // Send message to consumers
  for(const auto& consumer: this->operator_->consumers()){
    caf::actor actorHandle = consumer.second->actorHandle();
    operatorActor->send(actorHandle, e);
  }

  // Send message to root actor
  operatorActor->send(rootActor_, e);
}

} // namespace
