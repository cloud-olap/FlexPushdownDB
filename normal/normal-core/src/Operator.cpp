//
// Created by matt on 5/12/19.
//

#include "normal/core/Operator.h"

#include <cassert>               // for assert
#include <utility>                // for move

#include "spdlog/spdlog.h"        // for info, error, warn

#include "normal/core/Message.h"  // for Message


void Operator::start() {

  spdlog::info("{}  |  Starting", this->m_name);

  m_running = true;
  this->onStart();

  spdlog::info("{}  |  Started", this->m_name);
}

void Operator::stop() {
  spdlog::info("{}  |  Stopping", this->m_name);

  this->onStop();
  m_running = false;

  spdlog::info("{}  |  Stopped", this->m_name);
}

bool Operator::running() {
  return m_running;
}

std::string &Operator::name() {
  return m_name;
}

Operator::Operator(std::string name) {
  m_name = std::move(name);
}

Operator::~Operator() = default;

void Operator::produce(const std::shared_ptr<Operator> &op) {
  m_consumers.emplace_back(op);
}

void Operator::consume(const std::shared_ptr<Operator> &op) {
  m_producers.emplace_back(op);
}

std::vector<std::shared_ptr<Operator>> Operator::consumers() {
  return m_consumers;
}

std::shared_ptr<OperatorContext> Operator::ctx() {
  assert(m_operatorContext);

  return m_operatorContext;
}
void Operator::receive(std::unique_ptr<Message> msg) {
  spdlog::info("{}  |  Receiving", this->m_name);

  if (!m_created)
    spdlog::error("{}  |  Cannot receive, operator is not created", this->m_name);
  else
    this->onReceive(std::move(msg));

  spdlog::info("{}  |  Received", this->m_name);
}

void Operator::create(std::shared_ptr<OperatorContext> ctx) {
  assert (ctx);

  spdlog::info("{}  |  Creating", this->m_name);

  m_operatorContext = std::move(ctx);
  m_created = true;

  spdlog::info("{}  |  Created", this->m_name);

  assert (m_operatorContext);
}

void Operator::onReceive(std::unique_ptr<Message> msg) {
  spdlog::warn("{}  |  Ignoring message, Operator is not a reactive operator (msg: {})",
      this->m_name, typeid(msg).name());
}

void Operator::onComplete(const Operator &op) {
  spdlog::warn("{}  |  Ignoring message, Operator does not implement onComplete (Completed operator: {})",
               this->m_name, typeid(op).name());
}

void Operator::complete(const Operator &consumer) {
  onComplete(consumer);
}


