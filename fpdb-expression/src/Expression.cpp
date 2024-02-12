//
// Created by matt on 21/4/20.
//

#include <fpdb/expression/Expression.h>

using namespace fpdb::expression;

const std::shared_ptr<arrow::DataType> &Expression::getReturnType() const {
  return returnType_;
}
