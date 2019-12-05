//
// Created by matt on 5/12/19.
//

#include <iostream>
#include <utility>
#include <spdlog/spdlog.h>
#include "normal/core/Operator.h"

void Operator::start(std::shared_ptr<OperatorContext> ctx) {
  spdlog::info("{}  |  Starting", this->m_name);
  m_operatorContext = std::move(ctx);
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
void Operator::produce(const std::shared_ptr<Operator>& op) {
  m_consumers.emplace_back(op);
}
void Operator::consume(const std::shared_ptr<Operator>& op) {
  m_producers.emplace_back(op);
}
std::vector<std::shared_ptr<Operator>> Operator::consumers() {
  return m_consumers;
}

std::shared_ptr<OperatorContext> Operator::ctx() {
  return m_operatorContext;
}
