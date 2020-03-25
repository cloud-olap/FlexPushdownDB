//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_OPERATORMANAGER_H
#define NORMAL_NORMAL_CORE_OPERATORMANAGER_H

//#include <map>
//#include <string>
//
////#include <caf/all.hpp>
////#include <caf/io/all.hpp>
//
//#include "normal/core/Operator.h"
//#include "normal/core/Message.h"
//#include "normal/core/OperatorContext.h"

#include <map>
#include <string>
#include <memory>

#include <caf/all.hpp>

#include "OperatorContext.h"
#include "OperatorDirectory.h"

namespace normal::core {

class OperatorManager {
private:
  std::map<std::string, std::shared_ptr<normal::core::OperatorContext>> m_operatorMap;
  caf::actor_system_config actorSystemConfig;
  std::unique_ptr<caf::actor_system> actorSystem;
  std::map<std::string, caf::actor_id> actorMap;
  std::shared_ptr<caf::scoped_actor> rootActor_;
  normal::core::OperatorDirectory operatorDirectory_;
public:
  void put(const std::shared_ptr<normal::core::Operator> &op);
  void boot();
  void start();
  void stop();
  void join();

  OperatorManager();
};

}

#endif //NORMAL_NORMAL_CORE_OPERATORMANAGER_H
