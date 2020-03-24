//
// Created by matt on 24/3/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORYENTRY_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORYENTRY_H

#include <string>

namespace normal::core {

class OperatorDirectoryEntry {

private:
  std::string name_;
  bool complete_;

public:
  OperatorDirectoryEntry(std::string name,
                         bool complete);
  [[nodiscard]] bool complete() const;
  void complete(bool complete);
  [[nodiscard]] const std::string &name() const;
};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_OPERATORDIRECTORYENTRY_H
