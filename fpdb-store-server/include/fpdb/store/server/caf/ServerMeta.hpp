//
// Created by matt on 11/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_SERVERMETA_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_SERVERMETA_HPP

#include <caf/typed_actor.hpp>
#include "AbstractActor.hpp"
#include "fpdb/store/server/Global.hpp"

CAF_BEGIN_TYPE_ID_BLOCK(Server, fpdb::store::server::FPDB_STORE_FIRST_CAF_TYPE_ID)

// Event fired when server coming online
CAF_ADD_ATOM(Server, NodeUpAtom)

// Event fired when server goes offline (may not be sent due to crash of course)
CAF_ADD_ATOM(Server, NodeDownAtom)

CAF_ADD_ATOM(Server, DummyAtom)

CAF_END_TYPE_ID_BLOCK(Server)

namespace fpdb::store::server::cluster {

using namespace fpdb::store::server::caf;

using ClusterActor =
  AbstractActor::extend_with<::caf::typed_actor<::caf::result<void>(NodeUpAtom), ::caf::result<void>(NodeDownAtom)>>;

class ClusterActorState; // Forward declaration
using ClusterStatefulActor = ClusterActor::stateful_pointer<ClusterActorState>;

using NodeActor =
  AbstractActor::extend_with<::caf::typed_actor<::caf::result<void>(DummyAtom)>>;

class NodeActorState; // Forward declaration
using NodeStatefulActor = NodeActor::stateful_pointer<NodeActorState>;

} // namespace fpdb::store::server::caf

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_SERVERMETA_HPP
