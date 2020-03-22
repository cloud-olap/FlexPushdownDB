//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorContext.h"

#include <utility>
#include <cassert>

#include "normal/core/Globals.h"
#include "normal/core/Message.h"          // for Message
#include "normal/core/OperatorManager.h"  // for OperatorManager

namespace normal::core {

void OperatorContext::tell(std::shared_ptr<normal::core::Message> &msg) {

  assert(this);

  OperatorActor* oa = this->operatorActor();

  for(const auto& consumer: this->operator_->consumers()){
    normal::core::Envelope e(msg);
    caf::actor actorHandle = consumer.second->actorHandle();
    auto* otherActor = caf::actor_cast<OperatorActor*>(actorHandle);
    oa->send(otherActor, e);
  }
}

OperatorContext::OperatorContext(std::shared_ptr<normal::core::Operator> op) {

  assert(op);
//  assert(mgr);

  operator_ = std::move(op);
//  operatorManager_ = std::move(mgr);
}

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

} // namespace
