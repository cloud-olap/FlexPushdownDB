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
  std::unordered_map <std::string, LocalOperatorDirectoryEntry> entries_;

  int numProducers = 0;
  int numConsumers = 0;
  int numProducersComplete = 0;
  int numConsumersComplete = 0;


public:
  void insert(const LocalOperatorDirectoryEntry &entry);
  void setComplete(const std::string &name);
  bool allComplete(const OperatorRelationshipType &operatorRelationshipType) const;
  [[nodiscard]] std::string showString() const;

  [[maybe_unused]] void setIncomplete();

  tl::expected<LocalOperatorDirectoryEntry, std::string>
  get(const std::string& operatorId);
  std::vector<LocalOperatorDirectoryEntry>
  get(const OperatorRelationshipType &operatorRelationshipType);
  void destroyActorHandles();

};

}

#endif //NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H
