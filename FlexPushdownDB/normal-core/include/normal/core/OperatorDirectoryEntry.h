//
// Created by matt on 24/3/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORYENTRY_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORYENTRY_H

#include <string>

#include <caf/all.hpp>

#include <normal/core/OperatorContext.h>

namespace normal::core {

/**
 * Entry in the operator directory
 */
class OperatorDirectoryEntry {

private:
  std::shared_ptr<Operator> def_;
  caf::actor actorHandle_;
  bool complete_;

public:
  OperatorDirectoryEntry(std::shared_ptr<Operator> def, caf::actor actorHandle, bool complete);
  [[nodiscard]] const std::shared_ptr<Operator> &getDef() const;
  [[nodiscard]] const caf::actor &getActorHandle() const;
  [[nodiscard]] bool isComplete() const;
  void setDef(const std::shared_ptr<Operator> &def);
  void setActorHandle(const caf::actor &actorHandle);
  void setComplete(bool complete);
};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORYENTRY_H
