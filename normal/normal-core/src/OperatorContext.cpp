//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorContext.h"

#include <utility>
#include <cassert>

#include "normal/core/Message.h"          // for Message
#include "normal/core/OperatorManager.h"  // for OperatorManager

namespace normal::core {

void OperatorContext::tell(Message &msg) {

  assert(this);

//  operatorManager_->tell(msg, operator_);
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

void OperatorContext::complete() {
//  this->operatorManager_->complete(*this->operator_);
}

std::map<std::string, normal::core::OperatorMeta> &OperatorContext::operatorMap() {
  return operatorMap_;
}

} // namespace
