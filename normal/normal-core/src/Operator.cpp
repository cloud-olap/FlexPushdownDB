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

//void Operator::start() {
//
//  SPDLOG_DEBUG("{}  |  Starting", this->m_name);
//
//  m_running = true;
//  this->onStart();
//
//  SPDLOG_DEBUG("{}  |  Started", this->m_name);
//}
//
//void Operator::stop() {
//  SPDLOG_DEBUG("{}  |  Stopping", this->m_name);
//
//  this->onStop();
//  m_running = false;
//
//  SPDLOG_DEBUG("{}  |  Stopped", this->m_name);
//}

//bool Operator::isRunning() {
//  return m_running;
//}

std::string &Operator::name() {
  return m_name;
}

Operator::Operator(std::string name) {
  m_name = std::move(name);
}

Operator::~Operator() = default;

void Operator::produce(const std::shared_ptr<Operator> &op) {
  m_consumers.emplace(op->name(), op);
}

void Operator::consume(const std::shared_ptr<Operator> &op) {
  m_producers.emplace(op->name(), op);
}

std::map<std::string, std::shared_ptr<Operator>> Operator::consumers() {
  return m_consumers;
}

std::map<std::string, std::shared_ptr<Operator>> Operator::producers() {
  return m_producers;
}

std::shared_ptr<OperatorContext> Operator::ctx() {
  assert(m_operatorContext);

  return m_operatorContext;
}

void Operator::create(std::shared_ptr<OperatorContext> ctx) {
  assert (ctx);

  SPDLOG_DEBUG("Creating operator '{}'", this->m_name);

  m_operatorContext = std::move(ctx);

  SPDLOG_DEBUG("Created operator '{}'", this->m_name);

  assert (m_operatorContext);
}

void Operator::onReceive(const  normal::core::Envelope &msg) {
  SPDLOG_WARN("{}  |  Ignoring message, Operator is not a reactive operator (msg: {})",
               this->m_name,
               typeid(msg).name());
}

caf::actor Operator::actorHandle() const {
  return actorHandle_;
}

void Operator::actorHandle(caf::actor actorHandle) {
  this->actorHandle_ = actorHandle;
}

} // namespace

