//
// Created by matt on 5/12/19.
//

#include <utility>


#include "normal/core/OperatorContext.h"

void OperatorContext::tell(const std::string& msg) {
  m_mgr->tell(msg, m_op);
}

OperatorContext::OperatorContext(std::shared_ptr<Operator> op){
  m_op = std::move(op);
}
std::shared_ptr<Operator> OperatorContext::op() {
  return m_op;
}
