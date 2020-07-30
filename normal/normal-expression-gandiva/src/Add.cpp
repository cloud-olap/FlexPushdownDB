//
// Created by matt on 28/4/20.
//

#include <gandiva/tree_expr_builder.h>
#include "normal/expression/gandiva/Add.h"

using namespace normal::expression::gandiva;

Add::Add(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
	: left_(std::move(left)), right_(std::move(right)) {
}

void Add::compile(std::shared_ptr<arrow::Schema> schema) {
  left_->compile(schema);
  right_->compile(schema);

  auto leftGandivaExpression = left_->getGandivaExpression();
  auto rightGandivaExpression = right_->getGandivaExpression();

  returnType_ = left_->getReturnType();

  auto addExpression = ::gandiva::TreeExprBuilder::MakeFunction(
	  "add",
	  {leftGandivaExpression, rightGandivaExpression},
	  returnType_);

  gandivaExpression_ = addExpression;
}

std::string Add::alias() {
  return "?column?";
}

std::shared_ptr<Expression> normal::expression::gandiva::plus(std::shared_ptr<Expression> left,
															  std::shared_ptr<Expression> right) {
  return std::make_shared<Add>(std::move(left), std::move(right));
}
