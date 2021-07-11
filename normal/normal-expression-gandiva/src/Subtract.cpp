//
// Created by matt on 28/4/20.
//

#include <gandiva/tree_expr_builder.h>

#include <utility>
#include "normal/expression/gandiva/Subtract.h"

using namespace normal::expression::gandiva;

Subtract::Subtract(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right) :
	BinaryExpression(std::move(left), std::move(right)) {
}

void Subtract::compile(std::shared_ptr<arrow::Schema> schema) {
  left_->compile(schema);
  right_->compile(schema);

  // FIXME: Verify both left and right are compatible types
  returnType_ = left_->getReturnType();

  auto function = "subtract";
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction(function, {left_->getGandivaExpression(), right_->getGandivaExpression()}, returnType_);
}

std::string Subtract::alias() {
  return "?column?";
}

std::shared_ptr<Expression> normal::expression::gandiva::minus(const std::shared_ptr<Expression>& left,
															   const std::shared_ptr<Expression>& right) {
  return std::make_shared<Subtract>(left, right);
}
