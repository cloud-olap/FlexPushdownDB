//
// Created by matt on 25/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORYENTRY_H
#define NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORYENTRY_H

#include <string>
#include <caf/all.hpp>
#include "OperatorRelationshipType.h"

namespace normal::core {

/**
 * An entry in the local operator directory
 */
class LocalOperatorDirectoryEntry {

private:
  std::string name_;
  std::optional<caf::actor> actor_;
  OperatorRelationshipType relationshipType_;
  bool complete_;

public:
  LocalOperatorDirectoryEntry(std::string name,
                              std::optional<caf::actor> actor,
                              OperatorRelationshipType relationshipType,
                              bool complete);


  [[nodiscard]] bool complete() const;
  void complete(bool complete);

  [[nodiscard]] const std::string &name() const;
  void name(const std::string &name);

  [[nodiscard]] OperatorRelationshipType relationshipType() const;
  void relationshipType(OperatorRelationshipType relationshipType);

};

}

#endif //NORMAL_NORMAL_CORE_SRC_LOCALOPERATORDIRECTORYENTRY_H
