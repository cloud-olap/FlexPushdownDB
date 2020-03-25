//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_OPERATORMANAGER_H
#define NORMAL_NORMAL_CORE_OPERATORMANAGER_H

#include <map>
#include <string>
#include <memory>

#include <caf/all.hpp>

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

public:
  OperatorManager();

  void put(const std::shared_ptr<Operator> &op);
  void boot();
  void start();
  void stop();
  void join();

};

}

#endif //NORMAL_NORMAL_CORE_OPERATORMANAGER_H
