//
// Created by matt on 5/12/19.
//

#include "normal/core/Operator.h"

#include <cassert>               // for assert
#include <utility>                // for move

#include "normal/core/Globals.h"
#include <normal/core/Envelope.h>
#include "normal/core/Message.h"  // for Message



namespace normal::core {

Operator::Operator(std::string name) {
  name_ = std::move(name);
}

Operator::~Operator() = default;

std::string &Operator::name() {
  return name_;
}

void Operator::produce(const std::shared_ptr<Operator> &op) {
  consumers_.emplace(op->name(), op);
}

void Operator::consume(const std::shared_ptr<Operator> &op) {
  producers_.emplace(op->name(), op);
}

std::map<std::string, std::shared_ptr<Operator>> Operator::consumers() {
  return consumers_;
}

std::map<std::string, std::shared_ptr<Operator>> Operator::producers() {
  return producers_;
}

std::shared_ptr<OperatorContext> Operator::ctx() {
  assert(opContext_);

  return opContext_;
}

void Operator::create(std::shared_ptr<OperatorContext> ctx) {
  assert (ctx);

  SPDLOG_DEBUG("Creating operator '{}'", this->name_);

  opContext_ = std::move(ctx);

  SPDLOG_DEBUG("Created operator '{}'", this->name_);

  assert (opContext_);
}

void Operator::onReceive(const  normal::core::Envelope &msg) {
  SPDLOG_WARN("{}  |  Ignoring message, Operator is not a reactive operator (msg: {})",
               this->name_,
               typeid(msg).name());
}

caf::actor Operator::actorHandle() const {
  return actorHandle_;
}

void Operator::actorHandle(caf::actor actorHandle) {
  this->actorHandle_ = std::move(actorHandle);
}

} // namespace

