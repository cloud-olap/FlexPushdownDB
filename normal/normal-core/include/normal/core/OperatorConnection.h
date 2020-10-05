//
// Created by matt on 23/9/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORCONNECTION_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORCONNECTION_H

#include <string>

#include <caf/all.hpp>

#include <normal/core/OperatorRelationshipType.h>

namespace normal::core {

class OperatorConnection {
public:
  OperatorConnection(std::string Name, caf::actor ActorHandle, OperatorRelationshipType ConnectionType);
private:
  std::string name_;
  caf::actor actorHandle_;
  OperatorRelationshipType connectionType_;
public:
  [[nodiscard]] const std::string &getName() const;
  [[nodiscard]] const caf::actor &getActorHandle() const;
  [[nodiscard]] OperatorRelationshipType getConnectionType() const;
};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORCONNECTION_H
