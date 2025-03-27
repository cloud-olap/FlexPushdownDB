//
// Created by matt on 24/3/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPDIRECTORY_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPDIRECTORY_H

#include <fpdb/executor/physical/POpDirectoryEntry.h>
#include <tl/expected.hpp>
#include <string>
#include <unordered_map>

namespace fpdb::executor::physical {

/**
 * Class for tracking physical operators from outside the actor system.
 */
class POpDirectory {
  using MapType = std::unordered_map<std::string, POpDirectoryEntry>;

private:
  MapType entries_;
  int numOperatorsToComplete_ = 0;    // num ops to finish, in adapt exec this may not be same as entries.size()
  int numOperatorsComplete_ = 0;
  int numOperatorsLeftOver_ = 0;      // num ops not finished after prev stage finishes

public:
  int numOperatorsLeftOver() const;

  tl::expected<void, std::string> insert(const POpDirectoryEntry& entry);
  tl::expected<POpDirectoryEntry, std::string> get(const std::string& name);
  void addNumOperatorsToComplete(int diff);
  void resetNumOps();

  tl::expected<void, std::string> setComplete(const std::string& name);
  void setIncomplete();
  [[nodiscard]] bool allComplete() const;

  [[nodiscard]] std::string showString() const;
  void clear();

  MapType::iterator begin();
  MapType::const_iterator begin() const;
  MapType::iterator end();
  MapType::const_iterator end() const;
  MapType::const_iterator cbegin() const;
  MapType::const_iterator cend() const;
  MapType::iterator erase(MapType::const_iterator pos);

};

}
#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPDIRECTORY_H
