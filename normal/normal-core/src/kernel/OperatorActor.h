//
// Created by Matt Youill on 31/12/19.
//

#ifndef NORMAL_OPERATORACTOR_H
#define NORMAL_OPERATORACTOR_H

#include <normal/core/Message.h>
#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "normal/core/Operator.h"
#include "StartMessage.h"

using OperatorActorType = caf::typed_actor<
    caf::replies_to<StartMessage>::with<void>
>;

class OperatorActor : public OperatorActorType::base {

public:
  explicit OperatorActor(caf::actor_config &cfg, const std::shared_ptr<Operator> &_operator);

  behavior_type make_behavior() override;
  std::shared_ptr<Operator> _operator;
};

#endif //NORMAL_OPERATORACTOR_H
