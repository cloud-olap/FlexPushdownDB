//
// Created by matt on 23/3/20.
//

#include "normal/core/Actors.h"

#include <caf/all.hpp>

namespace normal::core {

caf::actor Actors::toActorHandle(const std::shared_ptr<caf::scoped_actor> &a) {
  return caf::actor_cast<caf::actor>(*a);
}

std::shared_ptr<caf::actor> Actors::toActorHandleShared(const std::shared_ptr<caf::scoped_actor> &a) {
  return reinterpret_cast<const std::shared_ptr<caf::actor> &>(*a);
}

}
