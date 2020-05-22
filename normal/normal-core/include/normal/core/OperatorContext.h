//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
#define NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H

#include <memory>
#include <string>

#include "normal/core/Operator.h"
#include "normal/core/OperatorActor.h"
#include "normal/core/LocalOperatorDirectory.h"
#include "normal/core/message/Message.h"

namespace normal::core {

class OperatorActor;
class Operator;

/**
 * The API operators use to interact with their environment, e.g. sending messages
 */
class OperatorContext {
private:
  std::shared_ptr<Operator> operator_;
  OperatorActor* operatorActor_;
  LocalOperatorDirectory operatorMap_;
  caf::actor rootActor_;

public:
  OperatorContext(std::shared_ptr<Operator> op, caf::actor& rootActor);

  std::shared_ptr<Operator> op();

  OperatorActor* operatorActor();
  void operatorActor(OperatorActor *operatorActor);

  LocalOperatorDirectory &operatorMap();

  void tell(std::shared_ptr<message::Message> &msg);

  void notifyComplete();

  tl::expected<void, std::string> send(const std::shared_ptr<message::Message> &msg, const std::string &recipientId);
};

}

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
