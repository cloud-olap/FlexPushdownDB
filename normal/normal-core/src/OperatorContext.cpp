//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorContext.h"

#include <utility>
#include <cassert>

#include "normal/core/Message.h"          // for Message
#include "normal/core/OperatorManager.h"  // for OperatorManager

void OperatorContext::tell(std::unique_ptr<Message> msg) {

  assert(this);
  assert(msg);

  m_mgr->tell(std::move(msg), m_op);
}

OperatorContext::OperatorContext(std::shared_ptr<Operator> op, std::shared_ptr<OperatorManager> mgr) {

  assert(op);
  assert(mgr);

  m_op = std::move(op);
  m_mgr = std::move(mgr);
}

std::shared_ptr<Operator> OperatorContext::op() {
  return m_op;
}
