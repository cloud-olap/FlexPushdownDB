//
// Created by matt on 27/4/20.
//

#include "normal/expression/simple/Expression.h"

using namespace normal::expression::simple;

const std::shared_ptr<arrow::DataType> &Expression::getReturnType() const {
  return returnType_;
}