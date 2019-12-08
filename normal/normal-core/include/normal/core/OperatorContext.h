//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
#define NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H

#include "normal/core/OperatorManager.h"
#include <map>
#include <memory>
#include <string>

class OperatorManager;
class Operator;

class OperatorContext {
private:
  std::shared_ptr<OperatorManager> m_mgr;
  std::shared_ptr<Operator> m_op;
public:
  explicit OperatorContext(std::shared_ptr<Operator> op);

  std::shared_ptr<Operator> op();

  void tell(const std::string &msg);
};

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
