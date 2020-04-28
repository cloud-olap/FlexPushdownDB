//
// Created by matt on 27/4/20.
//

#include "normal/expression/gandiva/Expression.h"

using namespace normal::expression::gandiva;

const gandiva::NodePtr &Expression::getGandivaExpression() const {
  return gandivaExpression_;
}

std::string Expression::showString() {
  return gandivaExpression_->ToString();
}
