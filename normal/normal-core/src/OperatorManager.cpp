//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorManager.h"

#include <cassert>
#include <vector>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <normal/core/Actors.h>

#include "normal/core/Globals.h"
#include "normal/core/message/Envelope.h"
#include "normal/core/Operator.h"
#include "normal/core/OperatorContext.h"
#include "normal/core/OperatorActor.h"
#include "normal/core/message/StartMessage.h"

namespace normal::core {

void OperatorManager::put(const std::shared_ptr<normal::core::Operator> &op) {

  assert(op);

  caf::actor rootActorHandle = normal::core::Actors::toActorHandle(this->rootActor_);

  auto ctx = std::make_shared<normal::core::OperatorContext>(op, rootActorHandle);
  m_operatorMap.insert(std::pair(op->name(), ctx));

  operatorDirectory_.insert(normal::core::OperatorDirectoryEntry(op->name(), std::optional<caf::actor>(), false));
}

void OperatorManager::start() {

  // Mark all the operators as incomplete
  operatorDirectory_.setIncomplete();


//   Send start messages to the actors
  for (const auto &element: m_operatorMap) {
    auto ctx = element.second;
    auto op = ctx->op();

    std::vector<caf::actor> actorHandles;
    for (const auto &consumer: op->consumers())
      actorHandles.emplace_back(consumer.second->actorHandle());

    auto sm = std::make_shared<normal::core::StartMessage>(actorHandles, "root");

    (*rootActor_)->send(op->actorHandle(), normal::core::Envelope(sm));
  }
}

void OperatorManager::stop() {
  // TODO: Send actors a shutdown message
  this->actorSystem->await_actors_before_shutdown(false);
}

OperatorManager::OperatorManager() {
  actorSystemConfig.load<caf::io::middleman>();
  actorSystem = std::make_unique<caf::actor_system>(actorSystemConfig);
  rootActor_ = std::make_shared<caf::scoped_actor>(*actorSystem);
}

void OperatorManager::join() {

  SPDLOG_DEBUG("Waiting for all operators to complete");

  auto handle_err = [&](const caf::error &err) {
    aout(*rootActor_) << "AUT (actor under test) failed: "
                      << (*rootActor_)->system().render(err) << std::endl;
  };

  bool allComplete = false;
  (*rootActor_)->receive_while([&] { return !allComplete; })(
      [&](const normal::core::Envelope &msg) {
        SPDLOG_DEBUG("Message received  |  actor: 'OperatorManager', messageKind: '{}', from: '{}'",
                     msg.message().type(), msg.message().sender());

        this->operatorDirectory_.setComplete(msg.message().sender());

        allComplete = this->operatorDirectory_.allComplete();

        SPDLOG_DEBUG(this->operatorDirectory_.showString());
        SPDLOG_DEBUG(allComplete);
      },
      handle_err);
}

void OperatorManager::boot() {
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
    caf::actor actorHandle = actorSystem->spawn<normal::core::OperatorActor>(op);
    op->actorHandle(actorHandle);
  }

  // Tell the actors who their producers are
  for (const auto &element: m_operatorMap) {
    auto ctx = element.second;
    auto op = ctx->op();
    for (const auto &producerEntry: op->producers()) {
      auto producer = producerEntry.second;
      auto entry = LocalOperatorDirectoryEntry(producer->name(), producer->actorHandle(), OperatorRelationshipType::Producer, false);
      ctx->operatorMap().insert(entry);
    }
  }

  // Tell the actors who their consumers are
  for (const auto &element: m_operatorMap) {
    auto ctx = element.second;
    auto op = ctx->op();
    for (const auto &consumerEntry: op->consumers()) {
      auto consumer = consumerEntry.second;
      auto entry = LocalOperatorDirectoryEntry(consumer->name(), consumer->actorHandle(), OperatorRelationshipType::Consumer, false);
      ctx->operatorMap().insert(entry);
    }
  }
}

}