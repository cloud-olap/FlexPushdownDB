//
// Created by Yifei Yang on 10/17/22.
//

#include <fpdb/executor/caf/CAFAdaptPushdownUtil.h>

namespace fpdb::executor::caf {

void CAFAdaptPushdownUtil::startDaemonAdaptPushdownActorSystem() {
  daemonAdaptPushdownActorSystemConfig_ = std::make_shared<::caf::actor_system_config>();
  daemonAdaptPushdownActorSystem_ = std::make_shared<::caf::actor_system>(*daemonAdaptPushdownActorSystemConfig_);
}

void CAFAdaptPushdownUtil::stopDaemonAdaptPushdownActorSystem() {
  if (daemonAdaptPushdownActorSystem_ != nullptr) {
    daemonAdaptPushdownActorSystem_->await_all_actors_done();
  }
}

}
