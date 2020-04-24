//
// Created by matt on 21/4/20.
//

#include <normal/expression/Expression.h>

using namespace normal::expression;

const std::shared_ptr<arrow::DataType> &Expression::getReturnType() const {
  return returnType_;
}

const gandiva::NodePtr &Expression::getGandivaExpression() const {
  return gandivaExpression_;
}
std::string Expression::showString() {
  return gandivaExpression_->ToString();
}
