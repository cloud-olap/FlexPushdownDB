//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
#define NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H

#include <memory>
#include "Operator.h"

class OperatorManager;
namespace normal::core{
  class Operator;
}
class Message;

class OperatorContext {
private:
  std::shared_ptr<OperatorManager> m_mgr;
  std::shared_ptr<normal::core::Operator> m_op;
public:
  OperatorContext(std::shared_ptr<normal::core::Operator> op, std::shared_ptr<OperatorManager> mgr);

  std::shared_ptr<normal::core::Operator> op();

  void tell(Message& msg);
  void complete();
};

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
