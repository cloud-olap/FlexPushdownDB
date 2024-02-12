//
// Created by Yifei Yang on 10/17/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_CAFADAPTPUSHDOWNUTIL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_CAFADAPTPUSHDOWNUTIL_H

#include <caf/all.hpp>

namespace fpdb::executor::caf {

class CAFAdaptPushdownUtil {

public:
  // A global actor system used for adaptive pushdown in each compute node, when pushdown falls back to pullup
  // (i.e. request reject by storage)
  inline static std::shared_ptr<::caf::actor_system_config> daemonAdaptPushdownActorSystemConfig_ = nullptr;
  inline static std::shared_ptr<::caf::actor_system> daemonAdaptPushdownActorSystem_ = nullptr;
  static void startDaemonAdaptPushdownActorSystem();
  static void stopDaemonAdaptPushdownActorSystem();
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_CAFADAPTPUSHDOWNUTIL_H
