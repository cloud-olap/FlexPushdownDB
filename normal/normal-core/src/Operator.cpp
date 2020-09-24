//
// Created by matt on 5/12/19.
//

#include "normal/core/Operator.h"

#include <cassert>               // for assert
#include <utility>                // for move

#include "normal/core/Globals.h"
#include <normal/core/message/Envelope.h>
#include "normal/core/message/Message.h"  // for Message

namespace normal::core {

Operator::Operator(std::string name, std::string type, long queryId) :
    name_(std::move(name)),
    type_(std::move(type)),
    queryId_(queryId) {
}

const std::string &Operator::getType() const {
  return type_;
}

std::string &Operator::name() {
  return name_;
}

void Operator::produce(const std::shared_ptr<Operator> &op) {
  consumers_.emplace(op->name(), op);
}

void Operator::consume(const std::shared_ptr<Operator> &op) {
  producers_.emplace(op->name(), op);
}

std::map<std::string, std::weak_ptr<Operator>> Operator::consumers() {
  return consumers_;
}

std::map<std::string, std::weak_ptr<Operator>> Operator::producers() {
  return producers_;
}

std::shared_ptr<OperatorContext> Operator::ctx() {
  assert(!opContext_.expired());
  return opContext_.lock();
}

void Operator::create(const std::shared_ptr<OperatorContext>& ctx) {
  assert (ctx);

  SPDLOG_DEBUG("Creating operator  |  name: '{}'", this->name_);

  opContext_ = ctx;

  assert (!opContext_.expired());
}

const caf::actor& Operator::actorHandle() const {
  return actorHandle_;
}

void Operator::actorHandle(caf::actor actorHandle) {
  this->actorHandle_ = std::move(actorHandle);
}

void Operator::setName(const std::string &Name) {
  name_ = Name;
}
void Operator::destroyActor() {
	destroy(this->actorHandle_);
}

long Operator::getQueryId() const {
  return queryId_;
}

} // namespace

