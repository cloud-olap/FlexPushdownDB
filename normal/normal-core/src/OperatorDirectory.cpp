//
// Created by matt on 24/3/20.
//

#include "normal/core/OperatorDirectory.h"

#include "normal/core/Globals.h"
#include <sstream>

#include <fmt/format.h>

namespace normal::core {

void OperatorDirectory::insert(const OperatorDirectoryEntry& entry) {
  auto inserted = entries_.emplace(entry.getDef()->name(), entry);
  if(!inserted.second)
    throw std::runtime_error(fmt::format("Operator '{}' already added to directory", entry.getDef()->name()));
  ++numOperators_;
}

void OperatorDirectory::setComplete(const std::string& name) {
  auto entry = entries_.find(name);
  if(entry == entries_.end())
    throw std::runtime_error("No entry for operator '" + name + "'");
  else {
    if (entry->second.isComplete()) {
      throw std::runtime_error("Entry for operator '" + name + "'" + "completes twice");
    }
    entry->second.setComplete(true);
  }
  ++numOperatorsComplete_;
}

bool OperatorDirectory::allComplete() {
  return numOperatorsComplete_ >= numOperators_;
}

std::string OperatorDirectory::showString() const {
  std::stringstream ss;
  for(const auto& entry : entries_){
    ss << entry.second.getDef().get() << ": " << entry.second.isComplete() << std::endl;
  }
  return ss.str();
}

void OperatorDirectory::setIncomplete() {
  for(auto& entry : entries_){
    entry.second.setComplete(false);
  }
}

tl::expected<OperatorDirectoryEntry, std::string> OperatorDirectory::get(const std::string& name) {
  auto entryIt = entries_.find(name);
  if(entryIt == entries_.end()){
	return tl::unexpected(fmt::format("Operator with name '{}' not found", name));
  }
  else{
	return entryIt->second;
  }
}

void OperatorDirectory::clear() {
  entries_.clear();
}

OperatorDirectory::MapType::iterator OperatorDirectory::begin() {
  return entries_.begin();
}

OperatorDirectory::MapType::const_iterator OperatorDirectory::begin() const {
  return entries_.begin();
}

OperatorDirectory::MapType::iterator OperatorDirectory::end() {
  return entries_.end();
}

OperatorDirectory::MapType::const_iterator OperatorDirectory::end() const {
  return entries_.end();
}

OperatorDirectory::MapType::const_iterator OperatorDirectory::cbegin() const {
  return entries_.cbegin();
}

OperatorDirectory::MapType::const_iterator OperatorDirectory::cend() const {
  return entries_.cend();
}

}