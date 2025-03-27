//
// Created by matt on 24/3/20.
//

#include <fpdb/executor/physical/POpDirectory.h>
#include <fpdb/executor/physical/Globals.h>
#include <fmt/format.h>
#include <sstream>

namespace fpdb::executor::physical {

int POpDirectory::numOperatorsLeftOver() const {
  return numOperatorsLeftOver_;
}

tl::expected<void, std::string> POpDirectory::insert(const POpDirectoryEntry& entry) {
  auto inserted = entries_.emplace(entry.getDef()->name(), entry);
  if (!inserted.second) {
    return tl::make_unexpected(fmt::format("Operator '{}' already added to directory", entry.getDef()->name()));
  }
  ++numOperatorsToComplete_;
  return {};
}

void POpDirectory::addNumOperatorsToComplete(int diff) {
  numOperatorsToComplete_ += diff;
}

void POpDirectory::resetNumOps() {
  numOperatorsToComplete_ = 0;
  numOperatorsComplete_ = 0;
  numOperatorsLeftOver_ = entries_.size();
};

tl::expected<void, std::string> POpDirectory::setComplete(const std::string& name) {
  auto entry = entries_.find(name);
  if (entry == entries_.end())
    return tl::make_unexpected("No entry for operator '" + name + "'");
  else {
    if (entry->second.isComplete()) {
      return tl::make_unexpected("Opdir: Entry for operator '" + name + "'" + "completes twice");
    }
    entry->second.setComplete(true);
  }
  if (TEMP_FIX_TPCH_Q21) {
    printf(" ");    // unsure why if without this, BCAST dist join hangs at TPC-H Q21 SF100
  }
  SPDLOG_DEBUG("%s\n", fmt::format("Completing operator  |  '{}'", name).c_str());
  ++numOperatorsComplete_;
  return {};
}

bool POpDirectory::allComplete() const {
  return numOperatorsComplete_ >= numOperatorsToComplete_;
}

std::string POpDirectory::showString() const {
  std::stringstream ss;
  for(const auto& entry : entries_){
    ss << entry.second.getDef().get() << ": " << entry.second.isComplete() << std::endl;
  }
  return ss.str();
}

void POpDirectory::setIncomplete() {
  for(auto& entry : entries_){
    entry.second.setComplete(false);
  }
}

tl::expected<POpDirectoryEntry, std::string> POpDirectory::get(const std::string& name) {
  auto entryIt = entries_.find(name);
  if(entryIt == entries_.end()){
	return tl::unexpected(fmt::format("Operator with name '{}' not found", name));
  }
  else{
	return entryIt->second;
  }
}

void POpDirectory::clear() {
  entries_.clear();
}

POpDirectory::MapType::iterator POpDirectory::begin() {
  return entries_.begin();
}

POpDirectory::MapType::const_iterator POpDirectory::begin() const {
  return entries_.begin();
}

POpDirectory::MapType::iterator POpDirectory::end() {
  return entries_.end();
}

POpDirectory::MapType::const_iterator POpDirectory::end() const {
  return entries_.end();
}

POpDirectory::MapType::const_iterator POpDirectory::cbegin() const {
  return entries_.cbegin();
}

POpDirectory::MapType::const_iterator POpDirectory::cend() const {
  return entries_.cend();
}

POpDirectory::MapType::iterator POpDirectory::erase(MapType::const_iterator pos) {
  return entries_.erase(pos);
}

}