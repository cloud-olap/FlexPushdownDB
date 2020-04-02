//
// Created by matt on 2/4/20.
//

#include "AggregateExpression.h"

#include <utility>
AggregateExpression::AggregateExpression(std::string Text) : text(std::move(Text)) {}
