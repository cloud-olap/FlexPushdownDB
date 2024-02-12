//
// Created by matt on 24/3/20.
//

#include <fpdb/executor/physical/LocalPOpDirectoryEntry.h>
#include <caf/all.hpp>
#include <utility>

namespace fpdb::executor::physical {

LocalPOpDirectoryEntry::LocalPOpDirectoryEntry(std::string name,
                                               caf::actor actor,
                                               POpRelationshipType relationshipType,
                                               int nodeId,
                                               bool complete) :
  name_(std::move(name)),
  actor_(std::move(actor)),
  relationshipType_(relationshipType),
  nodeId_(nodeId),
  complete_(complete) {}

const std::string &LocalPOpDirectoryEntry::name() const {
  return name_;
}

bool LocalPOpDirectoryEntry::complete() const {
  return complete_;
}

void LocalPOpDirectoryEntry::complete(bool complete) {
  complete_ = complete;
}

POpRelationshipType LocalPOpDirectoryEntry::relationshipType() const {
  return relationshipType_;
}

void LocalPOpDirectoryEntry::relationshipType(POpRelationshipType relationshipType) {
  relationshipType_ = relationshipType;
}

void LocalPOpDirectoryEntry::name(const std::string &name) {
  name_ = name;
}

const caf::actor &LocalPOpDirectoryEntry::getActor() const {
  return actor_;
}

int LocalPOpDirectoryEntry::getNodeId() const {
  return nodeId_;
}

void LocalPOpDirectoryEntry::destroyActor() {
	destroy(actor_);
}

}
