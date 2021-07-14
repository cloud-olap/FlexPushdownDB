//
// Created by matt on 27/4/20.
//

#include "normal/expression/gandiva/Expression.h"
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/Or.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/core/type/Integer32Type.h>
#include <normal/core/type/Float64Type.h>

using namespace normal::expression::gandiva;

const gandiva::NodePtr &Expression::getGandivaExpression() const {
  return gandivaExpression_;
}

std::string Expression::showString() {
  return gandivaExpression_->ToString();
}

std::shared_ptr<std::string> normal::expression::gandiva::removePrefixInt(const std::string& str) {
  if (str.substr(0, prefixInt_.size()) == prefixInt_) {
    return std::make_shared<std::string>(str.substr(prefixInt_.size(), str.size() - prefixInt_.size()));
  } else {
    return nullptr;
  }
}

std::shared_ptr<std::string> normal::expression::gandiva::removePrefixFloat(const std::string& str) {
  if (str.substr(0, prefixFloat_.size()) == prefixFloat_) {
    return std::make_shared<std::string>(str.substr(prefixFloat_.size(), str.size() - prefixFloat_.size()));
  } else {
    return nullptr;
  }
}

std::shared_ptr<normal::core::type::Type> getType(const std::shared_ptr<normal::expression::gandiva::Expression>& expr) {
  if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::Int32Type>)) {
    return normal::core::type::integer32Type();
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::Int64Type>)) {
    return normal::core::type::integer32Type();
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::FloatType>)) {
    return normal::core::type::float64Type();
  } else {
    return nullptr;
  }
}

std::shared_ptr<normal::expression::gandiva::Expression> normal::expression::gandiva::simpleCast(std::shared_ptr<normal::expression::gandiva::Expression> expr) {
  if (typeid(*expr) == typeid(normal::expression::gandiva::And)) {
    auto andExpr = std::static_pointer_cast<normal::expression::gandiva::And>(expr);
    return and_(simpleCast(andExpr->getLeft()), simpleCast(andExpr->getRight()));
  }

  else if (typeid(*expr) == typeid(normal::expression::gandiva::Or)) {
    auto orExpr = std::static_pointer_cast<normal::expression::gandiva::Or>(expr);
    return or_(simpleCast(orExpr->getLeft()), simpleCast(orExpr->getRight()));
  }

  else if (typeid(*expr) == typeid(normal::expression::gandiva::EqualTo)) {
    auto eqExpr = std::static_pointer_cast<normal::expression::gandiva::EqualTo>(expr);
    auto leftExpr = eqExpr->getLeft();
    auto rightExpr = eqExpr->getRight();
    auto leftType = getType(leftExpr);
    if (leftType) {
      return eq(leftExpr, normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getType(rightExpr);
      if (rightType) {
        return eq(normal::expression::gandiva::cast(leftExpr, rightType), rightExpr);
      }
    }
    return expr;
  }

  else if (typeid(*expr) == typeid(normal::expression::gandiva::GreaterThan)) {
    auto gtExpr = std::static_pointer_cast<normal::expression::gandiva::GreaterThan>(expr);
    auto leftExpr = gtExpr->getLeft();
    auto rightExpr = gtExpr->getRight();
    auto leftType = getType(leftExpr);
    if (leftType) {
      return gt(leftExpr, normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getType(rightExpr);
      if (rightType) {
        return gt(normal::expression::gandiva::cast(leftExpr, rightType), rightExpr);
      }
    }
    return expr;
  }

  else if (typeid(*expr) == typeid(normal::expression::gandiva::GreaterThanOrEqualTo)) {
    auto geExpr = std::static_pointer_cast<normal::expression::gandiva::GreaterThanOrEqualTo>(expr);
    auto leftExpr = geExpr->getLeft();
    auto rightExpr = geExpr->getRight();
    auto leftType = getType(leftExpr);
    if (leftType) {
      return gte(leftExpr, normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getType(rightExpr);
      if (rightType) {
        return gte(normal::expression::gandiva::cast(leftExpr, rightType), rightExpr);
      }
    }
    return expr;
  }

  else if (typeid(*expr) == typeid(normal::expression::gandiva::LessThan)) {
    auto ltExpr = std::static_pointer_cast<normal::expression::gandiva::LessThan>(expr);
    auto leftExpr = ltExpr->getLeft();
    auto rightExpr = ltExpr->getRight();
    auto leftType = getType(leftExpr);
    if (leftType) {
      return lt(leftExpr, normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getType(rightExpr);
      if (rightType) {
        return lt(normal::expression::gandiva::cast(leftExpr, rightType), rightExpr);
      }
    }
    return expr;
  }

  else if (typeid(*expr) == typeid(normal::expression::gandiva::LessThanOrEqualTo)) {
    auto leExpr = std::static_pointer_cast<normal::expression::gandiva::LessThanOrEqualTo>(expr);
    auto leftExpr = leExpr->getLeft();
    auto rightExpr = leExpr->getRight();
    auto leftType = getType(leftExpr);
    if (leftType) {
      return lte(leftExpr, normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getType(rightExpr);
      if (rightType) {
        return lte(normal::expression::gandiva::cast(leftExpr, rightType), rightExpr);
      }
    }
    return expr;
  }

  return expr;
}
