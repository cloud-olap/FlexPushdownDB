//
// Created by Matt Youill on 31/12/19.
//

#ifndef NORMAL_OPERATORACTOR_H
#define NORMAL_OPERATORACTOR_H

#include <memory>
#include <queue>

#include <caf/all.hpp>

#include "normal/core/message/Message.h"
#include "normal/core/Operator.h"
#include "normal/core/message/StartMessage.h"
#include <normal/core/Forward.h>

namespace normal::core {

using GetProcessingTimeAtom = caf::atom_constant<caf::atom("get-pt")>;

/**
 * Operator actor implements caf::actor and combines the operators behaviour and state
 */
class OperatorActor : public caf::event_based_actor {

private:
  std::shared_ptr<Operator> opBehaviour_;
  long processingTime_ = 0;
public:
  long getProcessingTime() const;
  void incrementProcessingTime(long time);
  bool running_ = false;
  std::string name_;
  std::queue<std::pair<caf::message, caf::strong_actor_ptr>> buffer_;
  std::optional<caf::strong_actor_ptr> overriddenMessageSender_;
public:
  OperatorActor(caf::actor_config &cfg, std::shared_ptr<Operator> opBehaviour);

  std::shared_ptr<Operator> operator_() const;

  caf::behavior make_behavior() override;

  void on_exit() override;

  const char* name() const override {
    return name_.c_str();
  }
};

}

#endif //NORMAL_OPERATORACTOR_H
