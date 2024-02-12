//
// Created by matt on 11/2/22.
//

#include "fpdb/store/server/cluster/ClusterActor.hpp"

namespace fpdb::store::server::cluster {

void ClusterActorState::set_state(ClusterStatefulActor self) {
  AbstractActorState<ClusterStatefulActor>::set_base_state(self);
}

void ClusterActorState::on_exit(ClusterStatefulActor /*self*/, const ::caf::error& /*reason*/) {
  // NOOP
}

void ClusterActorState::on_linked_exit(ClusterStatefulActor /*self*/, const ::caf::exit_msg& /*exit_message*/) {
  // NOOP
}

ClusterActor::behavior_type ClusterActorState::actor_functor(ClusterStatefulActor self) {
  return self->state.make_behaviour(self);
}

} // namespace fpdb::store::server::cluster