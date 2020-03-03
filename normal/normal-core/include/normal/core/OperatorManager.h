//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_OPERATORMANAGER_H
#define NORMAL_NORMAL_CORE_OPERATORMANAGER_H

#include <map>
#include <memory>
#include <string>
#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include "Operator.h"

namespace normal::core {
class Operator;
class Message;
class OperatorContext;
}

class OperatorManager : public std::enable_shared_from_this<OperatorManager> {
private:
  std::map<std::string, std::shared_ptr<normal::core::OperatorContext>> m_operatorMap;
  caf::actor_system_config actorSystemConfig;
  std::unique_ptr<caf::actor_system> actorSystem;
  std::map<std::string, caf::actor_id> actorMap;
public:
  void put(const std::shared_ptr<normal::core::Operator> &op);

  void start();
  void stop();

  void tell(normal::core::Message &msg, const std::shared_ptr<normal::core::Operator> &op);
  void complete(normal::core::Operator &op);
  OperatorManager();
};

#endif //NORMAL_NORMAL_CORE_OPERATORMANAGER_H
