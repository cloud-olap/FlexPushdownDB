//
// Created by matt on 25/3/20.
//


#include "normal/core/LocalOperatorDirectory.h"

#include <sstream>

#include <fmt/format.h>

#include <normal/core/Globals.h>

namespace normal::core {

std::condition_variable condVar;

void LocalOperatorDirectory::insert(const LocalOperatorDirectoryEntry& entry) {
  // map insert cannot cover the value for the same key, need to delete first
  auto iter = entries_.find(entry.name());
  if (iter != entries_.end()) {
    entries_.erase(iter);
  }
  entries_.emplace(entry.name(), entry);

  switch (entry.relationshipType()) {
  case OperatorRelationshipType::Producer: ++numProducers;
	break;
  case OperatorRelationshipType::Consumer: ++numConsumers;
	break;
  case OperatorRelationshipType::None: throw std::runtime_error("Unconnected operator not supported");
    break;
  }
}

void LocalOperatorDirectory::setComplete(const std::string& name) {
  auto entry = entries_.find(name);
  if(entry == entries_.end())
    throw std::runtime_error("No entry for actor '" + name + "'");
  else {
  if (entry->second.complete()) {
    throw std::runtime_error("LocalOpdir: Entry for operator '" + name + "'" + "completes twice");
  }
	entry->second.complete(true);
	switch (entry->second.relationshipType()) {
	case OperatorRelationshipType::Producer: ++numProducersComplete;
	  break;
	case OperatorRelationshipType::Consumer: ++numConsumersComplete;
	  break;
	case OperatorRelationshipType::None:throw std::runtime_error("Unconnected operator not supported");
	  break;
	}
  }
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
  numProducersComplete = 0;
  numConsumersComplete = 0;
}

bool LocalOperatorDirectory::allComplete(const OperatorRelationshipType &operatorRelationshipType) {
  switch (operatorRelationshipType) {
  case OperatorRelationshipType::Producer: return numProducersComplete >= numProducers;
  case OperatorRelationshipType::Consumer: return numConsumersComplete >= numConsumers;
  case OperatorRelationshipType::None:
    throw std::runtime_error("Unconnected operator not supported");
  }
}

void LocalOperatorDirectory::destroyActorHandles(){
  for(auto &entry: entries_){
	entry.second.destroyActor();
  }
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