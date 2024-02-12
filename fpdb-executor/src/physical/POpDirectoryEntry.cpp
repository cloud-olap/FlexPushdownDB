//
// Created by matt on 24/3/20.
//

#include <fpdb/executor/physical/POpDirectoryEntry.h>
#include <caf/all.hpp>
#include <utility>

namespace fpdb::executor::physical {

POpDirectoryEntry::POpDirectoryEntry(std::shared_ptr<PhysicalOp> def,
											   ::caf::actor actorHandle,
											   bool complete) :
	def_(std::move(def)),
	actorHandle_(std::move(actorHandle)),
	complete_(complete) {}

const std::shared_ptr<PhysicalOp> &POpDirectoryEntry::getDef() const {
  return def_;
}

const ::caf::actor &POpDirectoryEntry::getActorHandle() const {
  return actorHandle_;
}

bool POpDirectoryEntry::isComplete() const {
  return complete_;
}

void POpDirectoryEntry::setDef(const std::shared_ptr<PhysicalOp> &def) {
  def_ = def;
}

void POpDirectoryEntry::setActorHandle(const ::caf::actor &actorHandle) {
  actorHandle_ = actorHandle;
}

void POpDirectoryEntry::setComplete(bool complete) {
  complete_ = complete;
}

}
