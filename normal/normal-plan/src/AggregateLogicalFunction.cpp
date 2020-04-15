//
// Created by matt on 2/4/20.
//

#include <normal/plan/AggregateLogicalFunction.h>

normal::plan::AggregateLogicalFunction::AggregateLogicalFunction(std::string type)
    : type_(std::move(type)) {
}

std::shared_ptr<normal::core::expression::Expression> normal::plan::AggregateLogicalFunction::expression() {
  return expression_;
}

void normal::plan::AggregateLogicalFunction::expression(const std::shared_ptr<normal::core::expression::Expression> &expression) {
  expression_ = expression;
}
