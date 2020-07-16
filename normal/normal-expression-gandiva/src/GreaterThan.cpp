//
// Created by Yifei Yang on 7/14/20.
//

#include "normal/expression/gandiva/GreaterThan.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

GreaterThan::GreaterThan(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
  : left_(std::move(Left)), right_(std::move(Right)) {}

void GreaterThan::compile(std::shared_ptr<arrow::Schema> schema) {

  left_->compile(schema);
  right_->compile(schema);

  auto leftGandivaExpression = left_->getGandivaExpression();
  auto rightGandivaExpression = right_->getGandivaExpression();

  auto greaterThanFunction = ::gandiva::TreeExprBuilder::MakeFunction(
          "greater_than",
          {leftGandivaExpression, rightGandivaExpression},
          ::arrow::boolean());

  gandivaExpression_ = greaterThanFunction;
  returnType_ = ::arrow::boolean();
}

std::string normal::expression::gandiva::GreaterThan::alias() {
  return "?column?";
}

std::shared_ptr<Expression> normal::expression::gandiva::gt(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right) {
  return std::make_shared<GreaterThan>(std::move(Left), std::move(Right));
}
