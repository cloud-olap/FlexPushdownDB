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
#include <normal/core/Forward.h>

namespace normal::core {

/**
 * The API operators use to interact with their environment, e.g. sending messages
 */
class OperatorContext {
private:
  OperatorActor* operatorActor_;
  LocalOperatorDirectory operatorMap_;
  caf::actor rootActor_;
  caf::actor segmentCacheActor_;
  bool complete_ = false;

public:
  OperatorContext(caf::actor rootActor, caf::actor segmentCacheActor);

  OperatorActor* operatorActor();
  void operatorActor(OperatorActor *operatorActor);

  LocalOperatorDirectory &operatorMap();

  void tell(std::shared_ptr<message::Message> &msg);

  void notifyComplete();

  tl::expected<void, std::string> send(const std::shared_ptr<message::Message> &msg, const std::string &recipientId);

  void destroyActorHandles();
  [[nodiscard]] bool isComplete() const;
};

}

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORCONTEXT_H
