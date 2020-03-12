//
// Created by matt on 3/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_KERNEL_ACTORDEF_H
#define NORMAL_NORMAL_CORE_SRC_KERNEL_ACTORDEF_H

//#include <string>
//#include <caf/all.hpp>
//#include <caf/io/all.hpp>

#include <string>
#include <caf/all.hpp>

namespace normal::core {

/**
 * Used by actors to hold meta data about other actors.
 */
class OperatorMeta {
private:
  std::string name_;
  caf::actor actorHandle_;

public:
  OperatorMeta(std::string name, caf::actor actorHandle);
  [[nodiscard]] const caf::actor &actorHandle() const;
  [[nodiscard]] const std::string &name() const;

};

} // namespace

#endif //NORMAL_NORMAL_CORE_SRC_KERNEL_ACTORDEF_H
