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

class Message;
class Operator;  // lines 16-16
class OperatorContext;  // lines 17-17

class OperatorManager : public std::enable_shared_from_this<OperatorManager>{
private:
  std::map<std::string, std::shared_ptr<OperatorContext>> m_operatorMap;
  caf::actor_system_config actorSystemConfig;
  std::unique_ptr<caf::actor_system> actorSystem;
  std::map<std::string, caf::actor_id> actorMap;
public:
  void put(const std::shared_ptr<normal::core::Operator> &op);

  void start();
  void stop();

  void tell(Message& msg, const std::shared_ptr<normal::core::Operator> &op);
  void complete(normal::core::Operator &op);
  OperatorManager();
};

#endif //NORMAL_NORMAL_CORE_OPERATORMANAGER_H
