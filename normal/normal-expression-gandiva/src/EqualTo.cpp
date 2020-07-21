//
// Created by matt on 11/6/20.
//

#include "normal/expression/gandiva/EqualTo.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

EqualTo::EqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
	: BinaryExpression(Left, Right) {}

void EqualTo::compile(std::shared_ptr<arrow::Schema> Schema) {

  left_->compile(Schema);
  right_->compile(Schema);

  auto leftGandivaExpression = left_->getGandivaExpression();
  auto rightGandivaExpression = right_->getGandivaExpression();

  auto equalToExpression = ::gandiva::TreeExprBuilder::MakeFunction(
	  "equal",
	  {leftGandivaExpression, rightGandivaExpression},
	  ::arrow::boolean());

  gandivaExpression_ = equalToExpression;
  returnType_ = ::arrow::boolean();
}

std::string EqualTo::alias() {
  return genAliasForComparison("=");
}

std::shared_ptr<Expression> normal::expression::gandiva::eq(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right) {
  return std::make_shared<EqualTo>(std::move(Left), std::move(Right));
}
