//
// Created by Yifei Yang on 1/14/22.
//

#ifndef FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_ACTORSYSTEMCONFIG_H
#define FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_ACTORSYSTEMCONFIG_H

#include <fpdb/executor/physical/POpActor.h>
#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/caf-serialization/CAFPOpSerializer.h>
#include <fpdb/executor/cache/SegmentCacheActor.h>
#include <fpdb/cache/caf-serialization/CAFCachingPolicySerializer.h>
#include <caf/io/all.hpp>

using namespace fpdb::executor::physical;
using namespace fpdb::executor::cache;

namespace fpdb::main {

struct ActorSystemConfig: ::caf::actor_system_config {
  ActorSystemConfig(int port,
                    const std::vector<std::string> &nodeIps,
                    bool isServer):
    port_(port),
    nodeIps_(nodeIps),
    isServer_(isServer) {
    load<::caf::io::middleman>();
    add_actor_type<POpActor, ::caf::no_spawn_options, std::shared_ptr<PhysicalOp>&>("POpActor");
    add_actor_type<POpActor, ::caf::detached, std::shared_ptr<PhysicalOp>&>("POpActor-detached");
    add_actor_type("SegmentCacheActor", SegmentCacheActor::makeBehaviour);
  }

  int port_;
  std::vector<std::string> nodeIps_;
  bool isServer_;
};

};


#endif //FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_ACTORSYSTEMCONFIG_H
