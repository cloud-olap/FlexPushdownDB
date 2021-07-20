//
// Created by matt on 24/3/20.
//

#include "normal/core/OperatorDirectoryEntry.h"

#include <utility>
#include <caf/all.hpp>

namespace normal::core {

OperatorDirectoryEntry::OperatorDirectoryEntry(std::shared_ptr<Operator> def,
											   caf::actor actorHandle,
											   bool complete) :
	def_(std::move(def)),
	actorHandle_(std::move(actorHandle)),
	complete_(complete) {}

const std::shared_ptr<Operator> &OperatorDirectoryEntry::getDef() const {
  return def_;
}

const caf::actor &OperatorDirectoryEntry::getActorHandle() const {
  return actorHandle_;
}

bool OperatorDirectoryEntry::isComplete() const {
  return complete_;
}

void OperatorDirectoryEntry::setDef(const std::shared_ptr<Operator> &def) {
  def_ = def;
}

void OperatorDirectoryEntry::setActorHandle(const caf::actor &actorHandle) {
  actorHandle_ = actorHandle;
}

void OperatorDirectoryEntry::setComplete(bool complete) {
  complete_ = complete;
}

}
