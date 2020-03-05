//
// Created by Matt Youill on 31/12/19.
//

#ifndef NORMAL_OPERATORACTOR_H
#define NORMAL_OPERATORACTOR_H

#include <memory>

#include <caf/all.hpp>
//#include <caf/io/all.hpp>

#include "normal/core/Message.h"
#include "normal/core/Operator.h"

#include "StartMessage.h"

namespace normal::core {
  class Operator;
}

class OperatorActor : public caf::event_based_actor {
public:
  OperatorActor(caf::actor_config &cfg, std::shared_ptr<normal::core::Operator> _operator);

  caf::behavior make_behavior() override;
  std::shared_ptr<normal::core::Operator> _operator;
};

#endif //NORMAL_OPERATORACTOR_H
