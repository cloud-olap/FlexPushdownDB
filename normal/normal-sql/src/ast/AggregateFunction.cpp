//
// Created by matt on 2/4/20.
//

#include "AggregateFunction.h"

#include <utility>

AggregateFunction::AggregateFunction(std::string type)
    : type_(std::move(type)) {
}

std::shared_ptr<normal::core::expression::Expression> AggregateFunction::expression() {
  return expression_;
}

void AggregateFunction::expression(const std::shared_ptr<normal::core::expression::Expression> &expression) {
  expression_ = expression;
}
