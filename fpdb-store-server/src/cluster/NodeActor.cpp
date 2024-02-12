//
// Created by matt on 11/2/22.
//

#include "fpdb/store/server/cluster/NodeActor.hpp"

#include <caf/io/middleman.hpp>

namespace fpdb::store::server::cluster {

void NodeActorState::set_state(NodeStatefulActor self, std::optional<ClusterActor> cluster_actor_handle,
                               std::optional<std::string> cluster_actor_host, std::optional<int> cluster_actor_port) {
  AbstractActorState<NodeStatefulActor>::set_base_state(self);

  self->state.cluster_actor_handle_ = cluster_actor_handle;
  self->state.cluster_actor_host_ = cluster_actor_host;
  self->state.cluster_actor_port_ = cluster_actor_port;
}

void NodeActorState::on_exit(NodeStatefulActor /*self*/, const ::caf::error& /*reason*/) {
  // NOOP
}

void NodeActorState::on_linked_exit(NodeStatefulActor /*self*/, const ::caf::exit_msg& /*exit_message*/) {
  // NOOP
}

NodeActor::behavior_type
NodeActorState::actor_functor(NodeStatefulActor self, std::optional<ClusterActor> cluster_actor_handle,
                              std::optional<std::string> cluster_actor_host, std::optional<int> cluster_actor_port) {

  self->state.set_state(self, cluster_actor_handle, cluster_actor_host, cluster_actor_port);

  if(cluster_actor_handle){
    self->state.cluster_actor_handle_ = cluster_actor_handle.value();
  }
  else{
    auto expected_cluster_actor_handle = self->system().middleman().remote_actor(cluster_actor_host.value(), cluster_actor_port.value());
    self->state.cluster_actor_handle_ = ::caf::actor_cast<ClusterActor>(expected_cluster_actor_handle.value());
  }

  self->send(self->state.cluster_actor_handle_.value(), NodeUpAtom_v);

  return self->state.make_behaviour(self);
}

} // namespace fpdb::store::server::cluster