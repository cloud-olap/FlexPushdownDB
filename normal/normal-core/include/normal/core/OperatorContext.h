//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
#define NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H

#include <memory>
#include <string>

#include "Operator.h"
#include "OperatorActor.h"
#include "LocalOperatorDirectory.h"

//class OperatorManager;


namespace normal::core {

class OperatorActor;
class Operator;

class OperatorContext {
private:
  std::shared_ptr<normal::core::Operator> operator_;
  OperatorActor* operatorActor_;
  LocalOperatorDirectory operatorMap_;
  caf::actor rootActor_;

public:
  OperatorContext(std::shared_ptr<normal::core::Operator> op, caf::actor& rootActor);

  std::shared_ptr<normal::core::Operator> op();

  normal::core::OperatorActor* operatorActor();
  void operatorActor(normal::core::OperatorActor *operatorActor);

  LocalOperatorDirectory &operatorMap();

  void tell(std::shared_ptr<normal::core::Message> &msg);

  void notifyComplete();

};

}

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
