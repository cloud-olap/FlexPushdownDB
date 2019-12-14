//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
#define NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H

#include <memory>

class OperatorManager;
class Operator;
class Message;

class OperatorContext {
private:
  std::shared_ptr<OperatorManager> m_mgr;
  std::shared_ptr<Operator> m_op;
public:
  OperatorContext(std::shared_ptr<Operator> op, std::shared_ptr<OperatorManager> mgr);

  std::shared_ptr<Operator> op();

  void tell(std::unique_ptr<Message> msg);
};

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
