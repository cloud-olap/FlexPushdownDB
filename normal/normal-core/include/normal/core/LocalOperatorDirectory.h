//
// Created by matt on 25/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H
#define NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H

#include <unordered_map>
#include <string>
#include <tl/expected.hpp>

#include "LocalOperatorDirectoryEntry.h"

namespace normal::core {

/**
 * A directory that operators use to store information about other operators
 */
class LocalOperatorDirectory {

private:
  std::map <std::string, LocalOperatorDirectoryEntry> entries_;

public:
  void insert(const LocalOperatorDirectoryEntry &entry);
  void setComplete(const std::string &name);
  bool allComplete();
  bool allComplete(const OperatorRelationshipType &operatorRelationshipType);
  [[nodiscard]] std::string showString() const;
  void setIncomplete();

  tl::expected<LocalOperatorDirectoryEntry, std::string>
  get(const std::string& operatorId);
  std::vector<LocalOperatorDirectoryEntry>
  get(const OperatorRelationshipType &operatorRelationshipType);
  void destroyActorHandles();

};

}

#endif //NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H
