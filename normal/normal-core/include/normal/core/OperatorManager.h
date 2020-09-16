//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_OPERATORMANAGER_H
#define NORMAL_NORMAL_CORE_OPERATORMANAGER_H

#include <map>
#include <string>
#include <memory>

#include <caf/all.hpp>
#include <tl/expected.hpp>
#include <utility>
#include <normal/core/Globals.h>
#include <normal/core/cache/SegmentCacheActor.h>

#include "OperatorContext.h"
#include "OperatorDirectory.h"

using namespace normal::core::cache;

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
//  OperatorDirectory operatorDirectory_;
  caf::actor segmentCacheActor_;
  std::shared_ptr<CachingPolicy> cachingPolicy_;

  std::chrono::steady_clock::time_point startTime_;
  std::chrono::steady_clock::time_point stopTime_;

  std::atomic<long> queryCounter_;
  bool running_;

public:
  OperatorManager();
  explicit OperatorManager(std::shared_ptr<CachingPolicy>  cachingPolicy);

  virtual ~OperatorManager();
//  void put(const std::shared_ptr<Operator> &op);
  const caf::actor &getSegmentCacheActor() const;
  const std::shared_ptr<caf::actor_system> &getActorSystem() const;
  long nextQueryId();

  void boot();
  void start();
  void stop();
//  void join();

//  tl::expected<void, std::string> send(std::shared_ptr<message::Message> message, const std::string &recipientId);
  std::shared_ptr<normal::core::message::Message> receive();

//  void write_graph(const std::string &file);

  tl::expected<long, std::string> getElapsedTime();

//  std::string showMetrics();
  std::string showCacheMetrics();
  void clearCacheMetrics();

};

}

#endif //NORMAL_NORMAL_CORE_OPERATORMANAGER_H
