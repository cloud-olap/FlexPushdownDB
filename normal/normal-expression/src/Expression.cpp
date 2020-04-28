//
// Created by matt on 21/4/20.
//

#include <normal/expression/Expression.h>

using namespace normal::expression;

const std::shared_ptr<arrow::DataType> &Expression::getReturnType() const {
  return returnType_;
}
