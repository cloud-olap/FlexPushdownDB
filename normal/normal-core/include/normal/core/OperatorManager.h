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

namespace normal::core {
//class Operator;
//class Message;
//class OperatorContext;
}

class OperatorManager {
private:
  std::map<std::string, std::shared_ptr<normal::core::OperatorContext>> m_operatorMap;
  caf::actor_system_config actorSystemConfig;
  std::unique_ptr<caf::actor_system> actorSystem;
  std::map<std::string, caf::actor_id> actorMap;
  std::unique_ptr<caf::scoped_actor> actor_;
public:
  void put(const std::shared_ptr<normal::core::Operator> &op);

  void start();
  void stop();
  void join();

  OperatorManager();
};

#endif //NORMAL_NORMAL_CORE_OPERATORMANAGER_H
