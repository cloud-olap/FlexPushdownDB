//
// Created by matt on 11/2/22.
//

#include "fpdb/store/server/caf/ActorManager.hpp"

namespace fpdb::store::server::caf {

ActorManager::ActorManager() : actor_system_config_(std::make_shared<::caf::actor_system_config>()) {
}

const std::shared_ptr<::caf::actor_system>& ActorManager::actor_system() const {
  return actor_system_;
}

}