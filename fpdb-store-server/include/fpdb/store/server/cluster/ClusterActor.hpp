//
// Created by matt on 11/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CLUSTERCOORDINATORACTOR_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CLUSTERCOORDINATORACTOR_HPP

#include "fpdb/store/server/caf/ServerMeta.hpp"

namespace fpdb::store::server::cluster {

using namespace fpdb::store::server::caf;

class ClusterActorState : public AbstractActorState<ClusterStatefulActor> {

public:
  static ClusterActor::behavior_type actor_functor(ClusterStatefulActor self);

  void on_exit(ClusterStatefulActor self, const ::caf::error& reason) override;

  void on_linked_exit(ClusterStatefulActor self, const ::caf::exit_msg& exit_message) override;

private:
  void set_state(ClusterStatefulActor self);

  template<class... Handlers>
  ClusterActor::behavior_type make_behaviour(ClusterStatefulActor self, Handlers... handlers) {
    return AbstractActorState<ClusterStatefulActor>::make_base_behaviour(
      self, [=](NodeUpAtom /*atom*/) { return on_node_up(self); },
      [=](NodeDownAtom /*atom*/) { return on_node_down(self); }, std::move(handlers)...);
  }

  static ::caf::result<void> on_node_up(ClusterStatefulActor /*self*/) {
    return {};
  }

  static ::caf::result<void> on_node_down(ClusterStatefulActor /*self*/) {
    return {};
  }

public:
  static const inline char* name = "ClusterActor";

private:
};

} // namespace fpdb::store::server::cluster

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CLUSTERCOORDINATORACTOR_HPP
