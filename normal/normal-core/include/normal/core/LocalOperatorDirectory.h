//
// Created by matt on 25/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H
#define NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H

#include <unordered_map>
#include <string>
#include "LocalOperatorDirectoryEntry.h"

namespace normal::core {

class LocalOperatorDirectory {

private:
  std::unordered_map <std::string, normal::core::LocalOperatorDirectoryEntry> entries_;

public:
  void insert(const LocalOperatorDirectoryEntry &entry);
  void setComplete(const std::string &name);
  bool allComplete();
  bool allComplete(OperatorRelationshipType operatorRelationshipType);
  std::string showString() const;
  void setIncomplete();

};

}

#endif //NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORY_H
