//
// Created by matt on 24/3/20.
//

#include <sstream>
#include "normal/core/OperatorDirectory.h"

namespace normal::core {

void OperatorDirectory::insert(const OperatorDirectoryEntry& entry) {
  entries_.emplace(entry.name(), entry);
}

void OperatorDirectory::setComplete(const std::string& name) {
  auto entry = entries_.find(name);
  if(entry == entries_.end())
    throw std::runtime_error("No entry for actor '" + name + "'");
  else
    entry->second.complete(true);
}

bool OperatorDirectory::allComplete() {
  for(const auto& entry : entries_){
    if(!entry.second.complete())
      return false;
  }
  return true;
}

std::string OperatorDirectory::showString() const {
  std::stringstream ss;
  for(const auto& entry : entries_){
    ss << entry.second.name() << ": " << entry.second.complete() << std::endl;
  }
  return ss.str();
}

void OperatorDirectory::setIncomplete() {
  for(auto& entry : entries_){
    entry.second.complete(false);
  }
}

}