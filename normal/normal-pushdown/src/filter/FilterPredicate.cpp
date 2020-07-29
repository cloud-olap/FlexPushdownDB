//
// Created by matt on 6/5/20.
//

#include "normal/pushdown/filter/FilterPredicate.h"

#include <utility>
#include <normal/expression/gandiva/BinaryExpression.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/core/type/Integer32Type.h>
#include <normal/core/type/Integer64Type.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/Or.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>

using namespace normal::pushdown::filter;

[[maybe_unused]] FilterPredicate::FilterPredicate(std::shared_ptr<normal::expression::gandiva::Expression> Expr) :
	expr_(std::move(Expr)) {}

std::shared_ptr<FilterPredicate> FilterPredicate::make(const std::shared_ptr<normal::expression::gandiva::Expression>& Expr) {
  return std::make_shared<FilterPredicate>(Expr);
}

const std::shared_ptr<normal::expression::gandiva::Expression> &FilterPredicate::expression() const {
  return expr_;
}

std::shared_ptr<normal::core::type::Type> getType(std::shared_ptr<normal::expression::gandiva::Expression> expr) {
  if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::Int32Type>)) {
    return normal::core::type::integer32Type();
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::Int64Type>)) {
    return normal::core::type::integer64Type();
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::FloatType>)) {
    return normal::core::type::float64Type();
  } else {
    return nullptr;
  }
}

void FilterPredicate::simpleCast(std::shared_ptr<normal::expression::gandiva::Expression> expr) {
  if (typeid(*expr) == typeid(normal::expression::gandiva::And) || typeid(*expr) == typeid(normal::expression::gandiva::Or)) {
    auto biExpr = std::static_pointer_cast<normal::expression::gandiva::BinaryExpression>(expr);
    simpleCast(biExpr->getLeft());
    simpleCast(biExpr->getRight());
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::EqualTo)) {
    auto eqExpr = std::static_pointer_cast<normal::expression::gandiva::EqualTo>(expr);
    auto leftExpr = eqExpr->getLeft();
    auto rightExpr = eqExpr->getRight();
    auto leftType = getType(leftExpr);
    if (leftType) {
      eqExpr->setRight(normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getType(rightExpr);
      if (rightType) {
        eqExpr->setLeft(normal::expression::gandiva::cast(leftExpr, rightType));
      }
    }
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::GreaterThan)) {
    auto gtExpr = std::static_pointer_cast<normal::expression::gandiva::GreaterThan>(expr);
    auto leftExpr = gtExpr->getLeft();
    auto rightExpr = gtExpr->getRight();
    auto leftType = getType(leftExpr);
    if (leftType) {
      gtExpr->setRight(normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getType(rightExpr);
      if (rightType) {
        gtExpr->setLeft(normal::expression::gandiva::cast(leftExpr, rightType));
      }
    }
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::GreaterThanOrEqualTo)) {
    auto geExpr = std::static_pointer_cast<normal::expression::gandiva::GreaterThanOrEqualTo>(expr);
    auto leftExpr = geExpr->getLeft();
    auto rightExpr = geExpr->getRight();
    auto leftType = getType(leftExpr);
    if (leftType) {
      geExpr->setRight(normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getType(rightExpr);
      if (rightType) {
        geExpr->setLeft(normal::expression::gandiva::cast(leftExpr, rightType));
      }
    }
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::LessThan)) {
    auto ltExpr = std::static_pointer_cast<normal::expression::gandiva::LessThan>(expr);
    auto leftExpr = ltExpr->getLeft();
    auto rightExpr = ltExpr->getRight();
    auto leftType = getType(leftExpr);
    if (leftType) {
      ltExpr->setRight(normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getType(rightExpr);
      if (rightType) {
        ltExpr->setLeft(normal::expression::gandiva::cast(leftExpr, rightType));
      }
    }
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::LessThanOrEqualTo)) {
    auto leExpr = std::static_pointer_cast<normal::expression::gandiva::LessThanOrEqualTo>(expr);
    auto leftExpr = leExpr->getLeft();
    auto rightExpr = leExpr->getRight();
    auto leftType = getType(leftExpr);
    if (leftType) {
      leExpr->setRight(normal::expression::gandiva::cast(rightExpr, leftType));
    } else {
      auto rightType = getType(rightExpr);
      if (rightType) {
        leExpr->setLeft(normal::expression::gandiva::cast(leftExpr, rightType));
      }
    }
  }
}
