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
  std::string name_;
  std::shared_ptr<OperatorContext> operatorContext_;
  bool complete_;

public:
  OperatorDirectoryEntry(std::string name,
						 std::shared_ptr<OperatorContext> operatorContext,
                         bool complete);
  [[nodiscard]] bool complete() const;
  void complete(bool complete);
  [[nodiscard]] const std::string &name() const;
  [[nodiscard]] std::weak_ptr<OperatorContext> getOperatorContext() const;

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORYENTRY_H
