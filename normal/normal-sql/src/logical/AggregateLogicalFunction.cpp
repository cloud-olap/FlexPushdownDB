//
// Created by matt on 2/4/20.
//

#include <normal/sql/logical/AggregateLogicalFunction.h>

normal::sql::logical::AggregateLogicalFunction::AggregateLogicalFunction(std::string type)
    : type_(std::move(type)) {
}

std::shared_ptr<normal::core::expression::Expression> normal::sql::logical::AggregateLogicalFunction::expression() {
  return expression_;
}

void normal::sql::logical::AggregateLogicalFunction::expression(const std::shared_ptr<normal::core::expression::Expression> &expression) {
  expression_ = expression;
}
