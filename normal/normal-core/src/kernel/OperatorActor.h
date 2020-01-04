//
// Created by Matt Youill on 31/12/19.
//

#ifndef NORMAL_OPERATORACTOR_H
#define NORMAL_OPERATORACTOR_H


#include <caf/event_based_actor.hpp>
#include "normal/core/Operator.h"

//class Operator;

class OperatorActor : public caf::event_based_actor {
private:
    std::shared_ptr<Operator> _operator;
public:
    explicit OperatorActor(const std::shared_ptr<Operator>& _operator, caf::actor_config &cfg) : event_based_actor(cfg) {
        _operator = operator;
    }

    caf::behavior make_behavior() override {
        return {
                [](int a, int b) {
                    return a + b;
                },
                [](int a, int b) {
                    return a - b;
                }
        };
    }
};


#endif //NORMAL_OPERATORACTOR_H
