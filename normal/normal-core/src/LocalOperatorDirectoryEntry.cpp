//
// Created by matt on 24/3/20.
//

#include "normal/core/LocalOperatorDirectoryEntry.h"

#include <utility>
#include <caf/all.hpp>

namespace normal::core {

LocalOperatorDirectoryEntry::LocalOperatorDirectoryEntry(std::string name,
                                                         std::optional<caf::actor> actor,
                                                         OperatorRelationshipType relationshipType,
                                                         bool complete) :
    name_(std::move(name)),
    actor_(std::move(actor)),
    relationshipType_(relationshipType),
    complete_(complete) {}

const std::string &LocalOperatorDirectoryEntry::name() const {
  return name_;
}

bool LocalOperatorDirectoryEntry::complete() const {
  return complete_;
}

void LocalOperatorDirectoryEntry::complete(bool complete) {
  complete_ = complete;
}

OperatorRelationshipType LocalOperatorDirectoryEntry::relationshipType() const {
  return relationshipType_;
}

void LocalOperatorDirectoryEntry::relationshipType(OperatorRelationshipType relationshipType) {
  relationshipType_ = relationshipType;
}

void LocalOperatorDirectoryEntry::name(const std::string &name) {
  name_ = name;
}

const std::optional<caf::actor> &LocalOperatorDirectoryEntry::getActor() const {
  return actor_;
}

}
