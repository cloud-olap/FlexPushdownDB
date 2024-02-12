//
// Created by matt on 11/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CLUSTER_NODEACTOR_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CLUSTER_NODEACTOR_HPP

#include "fpdb/store/server/caf/ServerMeta.hpp"

namespace fpdb::store::server::cluster {

using namespace fpdb::store::server::caf;

class NodeActorState : public AbstractActorState<NodeStatefulActor> {

public:
  static NodeActor::behavior_type
  actor_functor(NodeStatefulActor self, std::optional<ClusterActor> cluster_actor_handle,
                std::optional<std::string> cluster_actor_host, std::optional<int> cluster_actor_port);

  void on_exit(NodeStatefulActor self, const ::caf::error& reason) override;

  void on_linked_exit(NodeStatefulActor self, const ::caf::exit_msg& exit_message) override;

private:
  void set_state(NodeStatefulActor self,  std::optional<ClusterActor> cluster_actor_handle,std::optional<std::string> cluster_actor_host,
                 std::optional<int> cluster_actor_port);

  template<class... Handlers>
  NodeActor::behavior_type make_behaviour(NodeStatefulActor self, Handlers... handlers) {
    return AbstractActorState<NodeStatefulActor>::make_base_behaviour(
      self, [=](DummyAtom /*atom*/) { return on_dummy(self); }, std::move(handlers)...);
  }

  static ::caf::result<void> on_dummy(NodeStatefulActor /*self*/) {
    return {};
  }

public:
  static const inline char* name = "NodeActor";

private:
  std::optional<std::string> cluster_actor_host_;
  std::optional<int> cluster_actor_port_;

  std::optional<ClusterActor> cluster_actor_handle_;
};

} // namespace fpdb::store::server::cluster

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CLUSTER_NODEACTOR_HPP
