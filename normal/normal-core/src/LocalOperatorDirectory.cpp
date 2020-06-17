//
// Created by matt on 25/3/20.
//

#include <normal/core/Globals.h>
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
    ss << "{name: " << entry.second.name() << ", complete: " << entry.second.complete() << "}" << std::endl;
  }
  return ss.str();
}

void LocalOperatorDirectory::setIncomplete() {
  for(auto& entry : entries_){
    entry.second.complete(false);
  }
}

bool LocalOperatorDirectory::allComplete(const OperatorRelationshipType &operatorRelationshipType) {
  SPDLOG_DEBUG("Local operator directory:\n{}", showString());
  for(const auto& entry : entries_){
    if(entry.second.relationshipType() == operatorRelationshipType && !entry.second.complete())
      return false;
  }
  return true;
}

tl::expected<LocalOperatorDirectoryEntry, std::string> LocalOperatorDirectory::get(const std::string &operatorId) {
  auto entryIt = entries_.find(operatorId);
  if(entryIt == entries_.end()){
    auto message = fmt::format("Operator with id '{}' not found", operatorId);
    SPDLOG_DEBUG(message);
	SPDLOG_DEBUG("Operator directory:\n{}", showString());
	return tl::unexpected(message);
  }
  else{
	return entryIt->second;
  }
}

std::vector<LocalOperatorDirectoryEntry> LocalOperatorDirectory::get(const OperatorRelationshipType &operatorRelationshipType) {
  std::vector<LocalOperatorDirectoryEntry> matchingEntries;
  for(const auto& operatorEntry: entries_){
    if(operatorEntry.second.relationshipType() == operatorRelationshipType){
	  matchingEntries.emplace_back(operatorEntry.second);
    }
  }
  return matchingEntries;
}

}