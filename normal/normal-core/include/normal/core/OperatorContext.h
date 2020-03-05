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

class OperatorActor;
namespace normal::core {

//class Message;
class Operator;

class OperatorContext {
private:
  std::shared_ptr<normal::core::Operator> operator_;
  OperatorActor* operatorActor_{};
public:
  OperatorActor* getOperatorActor();
  void setOperatorActor(OperatorActor *operatorActor);
private:
  std::map<std::string, normal::core::OperatorMeta> operatorMap_;

public:
  explicit OperatorContext(std::shared_ptr<normal::core::Operator> op);
  std::shared_ptr<normal::core::Operator> op();
  std::map<std::string, normal::core::OperatorMeta> &operatorMap();
  void tell(std::shared_ptr<normal::core::Message> &msg);
  void complete();

};

}

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
