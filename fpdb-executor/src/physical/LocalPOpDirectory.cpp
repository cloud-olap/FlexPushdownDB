//
// Created by matt on 25/3/20.
//

#include <fpdb/executor/physical/LocalPOpDirectory.h>
#include <spdlog/spdlog.h>
#include <sstream>

namespace fpdb::executor::physical {

tl::expected<void, std::string> LocalPOpDirectory::insert(const LocalPOpDirectoryEntry& entry) {
  auto inserted = entries_.emplace(entry.name(), entry);
  if (!inserted.second) {
    return tl::make_unexpected(fmt::format("Operator '{}' already added to local directory", entry.name()));
  }

  if (entry.relationshipType() == POpRelationshipType::Producer) {
    ++numProducers;
  } else {
    ++numConsumers;
  }
  return {};
}

tl::expected<void, std::string> LocalPOpDirectory::setComplete(const std::string& name) {
  auto entry = entries_.find(name);
  if (entry == entries_.end()) {
    return tl::make_unexpected("No entry for actor '" + name + "'");
  }
  else {
    if (entry->second.complete()) {
      return tl::make_unexpected("LocalOpdir: Entry for operator '" + name + "'" + "completes twice");
    }
    entry->second.complete(true);
    if (entry->second.relationshipType() == POpRelationshipType::Producer) {
      ++numProducersComplete;
    } else {
      ++numConsumersComplete;
    }
    return {};
  }
}

std::string LocalPOpDirectory::showString() const {
  std::stringstream ss;
  for(const auto& entry : entries_){
    ss << "{name: " << entry.second.name() << ", complete: " << entry.second.complete() << "}" << std::endl;
  }
  return ss.str();
}

bool LocalPOpDirectory::allComplete(const POpRelationshipType &operatorRelationshipType) const {
  if (operatorRelationshipType == POpRelationshipType::Producer) {
    return numProducersComplete >= numProducers;
  } else {
    return numConsumersComplete >= numConsumers;
  }
}

void LocalPOpDirectory::destroyActorHandles(){
  for(auto &entry: entries_){
	entry.second.destroyActor();
  }
}

tl::expected<LocalPOpDirectoryEntry, std::string> LocalPOpDirectory::get(const std::string &operatorId) {
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

std::vector<LocalPOpDirectoryEntry> LocalPOpDirectory::get(const POpRelationshipType &relationshipType) {
  std::vector<LocalPOpDirectoryEntry> matchingEntries;
  for(const auto& operatorEntry: entries_){
    if(operatorEntry.second.relationshipType() == relationshipType){
	  matchingEntries.emplace_back(operatorEntry.second);
    }
  }
  return matchingEntries;
}

}