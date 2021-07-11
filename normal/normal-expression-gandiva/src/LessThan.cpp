//
// Created by matt on 6/5/20.
//

#include "normal/expression/gandiva/LessThan.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

LessThan::LessThan(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
	: BinaryExpression(std::move(Left), std::move(Right)) {}

void LessThan::compile(std::shared_ptr<arrow::Schema> Schema) {

  left_->compile(Schema);
  right_->compile(Schema);

  auto leftGandivaExpression = left_->getGandivaExpression();
  auto rightGandivaExpression = right_->getGandivaExpression();

  auto lessThanFunction = ::gandiva::TreeExprBuilder::MakeFunction(
	  "less_than",
	  {leftGandivaExpression, rightGandivaExpression},
	  ::arrow::boolean());

  gandivaExpression_ = lessThanFunction;
  returnType_ = ::arrow::boolean();
}

std::string LessThan::alias() {
  return genAliasForComparison("<");
}

std::shared_ptr<Expression> normal::expression::gandiva::lt(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right) {
  return std::make_shared<LessThan>(Left, Right);
}
