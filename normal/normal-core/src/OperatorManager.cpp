//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorManager.h"

void OperatorManager::put(const std::shared_ptr<Operator> &op) {
  auto ctx = std::make_shared<OperatorContext>(op);
  m_operatorMap.insert(std::pair(op->name(), ctx));
}

void OperatorManager::start() {
  for (const auto &op: m_operatorMap) {
    op.second->op()->start(op.second);
  }
}

void OperatorManager::stop() {
  for (const auto &op: m_operatorMap) {
    op.second->op()->stop();
  }
}

void OperatorManager::tell(const std::string &msg, const std::shared_ptr<Operator> &fromOp) {
  const std::vector<std::shared_ptr<Operator>> &consumers = fromOp->consumers();
  for (const auto &op: consumers) {
    op->onReceive(msg);
  }
}
