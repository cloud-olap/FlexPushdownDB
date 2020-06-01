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

#include "OperatorContext.h"
#include "OperatorDirectory.h"

namespace normal::core {

/**
 * At the moment, this class is a bit of a god class handling the setup, running etc of operators.
 *
 * Needs to be rethought
 */
class OperatorManager {

private:
  std::map<std::string, std::shared_ptr<OperatorContext>> m_operatorMap;
  caf::actor_system_config actorSystemConfig;
  std::unique_ptr<caf::actor_system> actorSystem;
  std::map<std::string, caf::actor_id> actorMap;
  std::shared_ptr<caf::scoped_actor> rootActor_;
  OperatorDirectory operatorDirectory_;

  std::chrono::steady_clock::time_point startTime_;
  std::chrono::steady_clock::time_point stopTime_;

public:
  OperatorManager();

  void put(const std::shared_ptr<Operator> &op);
  std::shared_ptr<Operator> getOperator(const std::string &);
  std::map<std::string, std::shared_ptr<OperatorContext>> getOperators();

  void boot();
  void start();
  void stop();
  void join();

  tl::expected<void, std::string> send(std::shared_ptr<message::Message> message, const std::string& recipientId);
  std::shared_ptr<normal::core::message::Message> receive();

  void write_graph(const std::string& file);

  tl::expected<long, std::string> getElapsedTime();

  std::string showMetrics();

};

}

#endif //NORMAL_NORMAL_CORE_OPERATORMANAGER_H
