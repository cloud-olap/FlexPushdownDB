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
#include "kernel/OperatorActor.h"
#include "kernel/StartMessage.h"
#include "caf/make_type_erased_tuple_view.hpp"

void OperatorManager::put(const std::shared_ptr<Operator> &op) {

  assert(op);

  auto ctx = std::make_shared<OperatorContext>(op, shared_from_this());
  m_operatorMap.insert(std::pair(op->name(), ctx));
}

void OperatorManager::start() {
  for (const auto &element: m_operatorMap) {

    auto ctx = element.second;
    auto op = ctx->op();
    op->create(ctx);

    auto actorRef = actorSystem->spawn<OperatorActor>(op);

    caf::scoped_actor self{*actorSystem};
    StartMessage sm;

    self->send(actorRef, sm);
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
  for (const auto &c: consumers) {
    c->receive(std::move(msg));
  }
}

void OperatorManager::complete(Operator &op) {

  const std::vector<std::shared_ptr<Operator>> &consumers = op.consumers();

  for (const auto &consumer : consumers) {
    consumer->complete(op);
  }
}

OperatorManager::OperatorManager(){
  actorSystemConfig.load<caf::io::middleman>();
  actorSystem = std::make_unique<caf::actor_system>(actorSystemConfig);
}
