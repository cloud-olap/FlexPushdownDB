//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_OPERATORMANAGER_H
#define NORMAL_NORMAL_CORE_OPERATORMANAGER_H

#include <map>
#include <memory>
#include <string>

class Message;
class Operator;  // lines 16-16
class OperatorContext;  // lines 17-17

class OperatorManager {
private:
  std::map<std::string, std::shared_ptr<OperatorContext>> m_operatorMap;
public:
  void put(const std::shared_ptr<Operator> &op);

  void start();
  void stop();

  void tell(std::unique_ptr<Message> msg, const std::shared_ptr<Operator> &op);
};

#endif //NORMAL_NORMAL_CORE_OPERATORMANAGER_H
