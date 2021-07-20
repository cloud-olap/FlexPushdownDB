//
// Created by matt on 28/4/20.
//

#include <gandiva/tree_expr_builder.h>

#include <utility>
#include "normal/expression/gandiva/Divide.h"

using namespace normal::expression::gandiva;

Divide::Divide(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
	: BinaryExpression(std::move(left), std::move(right)) {
}

void Divide::compile(std::shared_ptr<arrow::Schema> schema ) {
  left_->compile(schema);
  right_->compile(schema);

  // FIXME: Verify both left and right are compatible types
  returnType_ = left_->getReturnType();

  auto function = "divide";
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction(function, {left_->getGandivaExpression(), right_->getGandivaExpression()}, returnType_);

}

std::string Divide::alias() {
  return "?column?";
}

std::shared_ptr<Expression> normal::expression::gandiva::divide(const std::shared_ptr<Expression>& left,
																const std::shared_ptr<Expression>& right) {
  return std::make_shared<Divide>(left, right);
}
