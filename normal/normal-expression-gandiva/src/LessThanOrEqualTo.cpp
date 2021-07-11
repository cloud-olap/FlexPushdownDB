//
// Created by matt on 11/6/20.
//

#include "normal/expression/gandiva/LessThanOrEqualTo.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

LessThanOrEqualTo::LessThanOrEqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
	: BinaryExpression(std::move(Left), std::move(Right)) {}

void LessThanOrEqualTo::compile(std::shared_ptr<arrow::Schema> Schema) {

  left_->compile(Schema);
  right_->compile(Schema);

  auto leftGandivaExpression = left_->getGandivaExpression();
  auto rightGandivaExpression = right_->getGandivaExpression();

  auto lessThanOrEqualToExpression = ::gandiva::TreeExprBuilder::MakeFunction(
	  "less_than_or_equal_to",
	  {leftGandivaExpression, rightGandivaExpression},
	  ::arrow::boolean());

  gandivaExpression_ = lessThanOrEqualToExpression;
  returnType_ = ::arrow::boolean();
}

std::string LessThanOrEqualTo::alias() {
  return genAliasForComparison("<=");
}

std::shared_ptr<Expression> normal::expression::gandiva::lte(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right) {
  return std::make_shared<LessThanOrEqualTo>(Left, Right);
}
