//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
#define NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H

#include <memory>
#include <string>

#include "Operator.h"
#include "OperatorMeta.h"
#include "OperatorActor.h"

//class OperatorManager;


namespace normal::core {

class OperatorActor;
class Operator;

class OperatorContext {
private:
  std::shared_ptr<normal::core::Operator> operator_;
  OperatorActor* operatorActor_;
  std::map<std::string, normal::core::OperatorMeta> operatorMap_;

public:
  explicit OperatorContext(std::shared_ptr<normal::core::Operator> op);

  std::shared_ptr<normal::core::Operator> op();

  normal::core::OperatorActor* operatorActor();
  void operatorActor(normal::core::OperatorActor *operatorActor);

  std::map<std::string, normal::core::OperatorMeta> &operatorMap();

  void tell(std::shared_ptr<normal::core::Message> &msg);

};

}

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
