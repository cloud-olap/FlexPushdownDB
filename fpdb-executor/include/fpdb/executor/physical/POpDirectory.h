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
  int numOperators_ = 0;
  int numOperatorsComplete_ = 0;

public:
  tl::expected<void, std::string> insert(const POpDirectoryEntry& entry);
  tl::expected<POpDirectoryEntry, std::string> get(const std::string& name);

  tl::expected<void, std::string> setComplete(const std::string& name);
  void setIncomplete();
  [[nodiscard]] bool allComplete() const;

  [[nodiscard]] std::string showString() const;
  void clear();

  MapType::iterator begin();
  [[nodiscard]] MapType::const_iterator begin() const;
  MapType::iterator end();
  [[nodiscard]] MapType::const_iterator end() const;

  [[maybe_unused]] [[nodiscard]] MapType::const_iterator cbegin() const;
  [[maybe_unused]] [[nodiscard]] MapType::const_iterator cend() const;

};

}
#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPDIRECTORY_H
