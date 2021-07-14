//
// Created by matt on 24/3/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORY_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORY_H

#include <string>
#include <unordered_map>
#include <tl/expected.hpp>

#include "OperatorDirectoryEntry.h"

namespace normal::core {

/**
 * Class for tracking operators from outside the actor system.
 *
 * WIP: Also serves as the data structure that owns shared pointers to operators. The OperatorDirectoryEntry class
 * only dispenses weak pointers (for the purpose of breaking cycles).
 */
class OperatorDirectory {
  using MapType = std::unordered_map<std::string, OperatorDirectoryEntry>;

private:
  MapType entries_;
  int numOperators_ = 0;
  int numOperatorsComplete_ = 0;

public:
  void insert(const OperatorDirectoryEntry& entry);
  tl::expected<OperatorDirectoryEntry, std::string> get(const std::string& name);

  void setComplete(const std::string& name);
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
#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORY_H
