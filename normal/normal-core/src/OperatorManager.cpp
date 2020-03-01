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
#include "caf/all.hpp"
#include "caf/io/all.hpp"



void OperatorManager::put(const std::shared_ptr<normal::core::Operator> &op) {

  assert(op);

  auto ctx = std::make_shared<OperatorContext>(op, shared_from_this());
  m_operatorMap.insert(std::pair(op->name(), ctx));
}

void OperatorManager::start() {

  // Create the operators
  for (const auto &element: m_operatorMap) {
    auto ctx = element.second;
    auto op = ctx->op();
    op->create(ctx);
  }

  // Create the operator actors
  for (const auto &element: m_operatorMap) {
    auto ctx = element.second;
    auto op = ctx->op();
    caf::actor actorRef = actorSystem->spawn<OperatorActor>(op);
    op->setActorId(actorRef->id());
    actorSystem->registry().put(actorRef.id(), actorRef);
  }

  caf::scoped_actor self{*actorSystem};

//   Send start messages to the actors
  for (const auto &element: m_operatorMap) {
    auto ctx = element.second;
    auto op = ctx->op();

    std::vector<caf::actor_id> actorIds;
    for(const auto &consumer: op->consumers())
      actorIds.emplace_back(consumer.second->getActorId());

    auto actorRef = actorSystem->registry().get<caf::actor>(op->getActorId());
    StartMessage sm(actorIds);
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

void OperatorManager::tell(Message& msg, const std::shared_ptr<normal::core::Operator> &op) {

  assert(op);

  const std::map<std::string, std::shared_ptr<normal::core::Operator>> &consumers = op->consumers();
  for (const auto &c: consumers) {
    c.second->receive(msg);

//    caf::scoped_actor self{*actorSystem};
//    self->send(op->getActorId(), msg);
  }
}

void OperatorManager::complete(normal::core::Operator &op) {

  const std::map<std::string, std::shared_ptr<normal::core::Operator>> &consumers = op.consumers();

  for (const auto &consumer : consumers) {
    consumer.second->complete(op);
  }
}

OperatorManager::OperatorManager(){
  actorSystemConfig.load<caf::io::middleman>();
  actorSystem = std::make_unique<caf::actor_system>(actorSystemConfig);
}
