//
// Created by matt on 25/3/20.
//

#include "normal/core/LocalOperatorDirectory.h"
#include <sstream>

namespace normal::core {

void LocalOperatorDirectory::insert(const LocalOperatorDirectoryEntry& entry) {
  entries_.emplace(entry.name(), entry);
}

void LocalOperatorDirectory::setComplete(const std::string& name) {
  auto entry = entries_.find(name);
  if(entry == entries_.end())
    throw std::runtime_error("No entry for actor '" + name + "'");
  else
    entry->second.complete(true);
}

bool LocalOperatorDirectory::allComplete() {
  for(const auto& entry : entries_){
    if(!entry.second.complete())
      return false;
  }
  return true;
}

std::string LocalOperatorDirectory::showString() const {
  std::stringstream ss;
  for(const auto& entry : entries_){
    ss << entry.second.name() << ": " << entry.second.complete() << std::endl;
  }
  return ss.str();
}

void LocalOperatorDirectory::setIncomplete() {
  for(auto& entry : entries_){
    entry.second.complete(false);
  }
}

bool LocalOperatorDirectory::allComplete(const OperatorRelationshipType &operatorRelationshipType) {
  for(const auto& entry : entries_){
    if(entry.second.relationshipType() == operatorRelationshipType && !entry.second.complete())
      return false;
  }
  return true;
}

}