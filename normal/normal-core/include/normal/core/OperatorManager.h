//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_OPERATORMANAGER_H
#define NORMAL_NORMAL_CORE_OPERATORMANAGER_H

#include <map>
#include <string>
#include <memory>

#include <caf/all.hpp>

#include <normal/cache/CachingPolicy.h>

#include <normal/core/Globals.h>
#include <normal/core/message/Message.h>

using namespace normal::core::message;
using namespace normal::cache;

namespace normal::core {

/**
 * At the moment, this class is a bit of a god class handling the setup, running etc of operators.
 *
 * Needs to be rethought
 */
class OperatorManager {

private:
  caf::actor_system_config actorSystemConfig;
  std::shared_ptr<caf::actor_system> actorSystem;
  std::shared_ptr<caf::scoped_actor> rootActor_;
  caf::actor segmentCacheActor_;
  std::shared_ptr<CachingPolicy> cachingPolicy_;
  std::atomic<long> queryCounter_;
  bool running_;

public:
  OperatorManager();
  explicit OperatorManager(std::shared_ptr<CachingPolicy> cachingPolicy);

  virtual ~OperatorManager();
  const caf::actor &getSegmentCacheActor() const;
  const std::shared_ptr<caf::actor_system> &getActorSystem() const;
  long nextQueryId();

  void boot();
  void start();
  void stop();

  std::string showCacheMetrics();
  void clearCacheMetrics();
  void clearCrtQueryMetrics();
  void clearCrtQueryShardMetrics();
  double getCrtQueryHitRatio();
  double getCrtQueryShardHitRatio();

};

}

#endif //NORMAL_NORMAL_CORE_OPERATORMANAGER_H
