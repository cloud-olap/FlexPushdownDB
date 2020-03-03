//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
#define NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H

#include <memory>
#include "Operator.h"
#include "normal/core/OperatorMeta.h"

class OperatorManager;

namespace normal::core {

class Message;
class Operator;

class OperatorContext {
private:
  std::shared_ptr<OperatorManager> operatorManager_;
  std::shared_ptr<Operator> operator_;
  std::map<std::string, OperatorMeta> operatorMap_;

public:
  OperatorContext(std::shared_ptr<Operator> op, std::shared_ptr<OperatorManager> mgr);
  std::shared_ptr<normal::core::Operator> op();
  std::map<std::string, OperatorMeta> &operatorMap();
  void tell(Message &msg);
  void complete();

};

}

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
