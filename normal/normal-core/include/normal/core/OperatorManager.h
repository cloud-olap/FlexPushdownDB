//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_OPERATORMANAGER_H
#define NORMAL_NORMAL_CORE_OPERATORMANAGER_H

#include "normal/core/Operator.h"
#include "normal/core/OperatorContext.h"
#include <map>
#include <memory>
#include <string>

class Operator;
class OperatorContext;

class OperatorManager {
private:
  std::map<std::string, std::shared_ptr<OperatorContext>> m_operatorMap;
public:
  void put(const std::shared_ptr<Operator> &op);
  void start();
  void stop();
  void tell(const std::string& msg, const std::shared_ptr<Operator>& Ptr);
};

#endif //NORMAL_NORMAL_CORE_OPERATORMANAGER_H
