//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorManager.h"
#include <cassert>

#include <cassert>
#include <utility>                        // for pair, move
#include <vector>                         // for vector

#include "normal/core/Message.h"          // for Message
#include "normal/core/Operator.h"         // for Operator
#include "normal/core/OperatorContext.h"  // for OperatorContext

void OperatorManager::put(const std::shared_ptr<Operator> &op) {

  assert(op);

  auto mgr = std::make_shared<OperatorManager>();
  mgr.reset(this);
  auto ctx = std::make_shared<OperatorContext>(op, mgr);
  m_operatorMap.insert(std::pair(op->name(), ctx));
}

void OperatorManager::start() {

  for (const auto &op: m_operatorMap) {
    op.second->op()->create(op.second);
  }

  for (const auto &op: m_operatorMap) {
    op.second->op()->start();
  }
}

void OperatorManager::stop() {
  for (const auto &op: m_operatorMap) {
    op.second->op()->stop();
  }
}

void OperatorManager::tell(std::unique_ptr<Message> msg, const std::shared_ptr<Operator> &op) {

  assert(op);

  const std::vector<std::shared_ptr<Operator>> &consumers = op->consumers();
  for (const auto &op: consumers) {
    op->receive(std::move(msg));
  }
}
