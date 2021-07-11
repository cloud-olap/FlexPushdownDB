//
// Created by matt on 28/4/20.
//

#include "normal/expression/gandiva/Multiply.h"

#include <gandiva/tree_expr_builder.h>

#include <utility>

using namespace normal::expression::gandiva;

Multiply::Multiply(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
	: BinaryExpression(std::move(left), std::move(right)) {
}

void Multiply::compile(std::shared_ptr<arrow::Schema> schema) {
  left_->compile(schema);
  right_->compile(schema);

  // FIXME: Verify both left and right are compatible types
  returnType_ = left_->getReturnType();

  auto function = "multiply";
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction(function, {left_->getGandivaExpression(), right_->getGandivaExpression()}, returnType_);
}

std::string Multiply::alias() {
  return "?column?";
}

std::shared_ptr<Expression> normal::expression::gandiva::times(const std::shared_ptr<Expression>& left,
															   const std::shared_ptr<Expression>& right) {
  return std::make_shared<Multiply>(left, right);
}

