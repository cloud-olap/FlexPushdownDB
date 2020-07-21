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

std::shared_ptr<std::string> Expression::removePrefixInt(std::string str) {
  if (str.substr(0, prefixInt_.size()) == prefixInt_) {
    return std::make_shared<std::string>(str.substr(prefixInt_.size(), str.size() - prefixInt_.size()));
  } else {
    return nullptr;
  }
}

std::shared_ptr<std::string> Expression::removePrefixFloat(std::string str) {
  if (str.substr(0, prefixFloat_.size()) == prefixFloat_) {
    return std::make_shared<std::string>(str.substr(prefixFloat_.size(), str.size() - prefixFloat_.size()));
  } else {
    return nullptr;
  }
}
