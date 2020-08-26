//
// Created by Matt Youill on 31/12/19.
//

#ifndef NORMAL_OPERATORACTOR_H
#define NORMAL_OPERATORACTOR_H

#include <memory>

#include <caf/all.hpp>

#include "normal/core/message/Message.h"
#include "normal/core/Operator.h"
#include "normal/core/message/StartMessage.h"

namespace normal::core {

class Operator;

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
public:
  OperatorActor(caf::actor_config &cfg, std::shared_ptr<Operator> opBehaviour);

  std::shared_ptr<Operator> operator_() const;

  caf::behavior make_behavior() override;

  void on_exit() override;
};

}

#endif //NORMAL_OPERATORACTOR_H
