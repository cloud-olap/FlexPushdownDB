//
// Created by matt on 11/6/20.
//

#include <gandiva/tree_expr_builder.h>
#include "normal/expression/gandiva/And.h"

using namespace normal::expression::gandiva;

And::And(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
	: left_(std::move(left)), right_(std::move(right)) {
}

void And::compile(std::shared_ptr<arrow::Schema> schema) {

  left_->compile(schema);
  right_->compile(schema);

  auto leftGandivaExpression = left_->getGandivaExpression();
  auto rightGandivaExpression = right_->getGandivaExpression();

  auto andExpression = ::gandiva::TreeExprBuilder::MakeAnd(
	  {leftGandivaExpression, rightGandivaExpression});

  gandivaExpression_ = andExpression;
  returnType_ = ::arrow::boolean();
}

std::string And::alias() {
  return "?column?";
}

std::shared_ptr<Expression> normal::expression::gandiva::and_(std::shared_ptr<Expression> left,
															  std::shared_ptr<Expression> right) {
  return std::make_shared<And>(std::move(left), std::move(right));
}
