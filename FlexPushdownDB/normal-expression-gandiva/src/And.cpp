//
// Created by matt on 11/6/20.
//

#include <gandiva/tree_expr_builder.h>
#include "normal/expression/gandiva/And.h"

using namespace normal::expression::gandiva;

And::And(const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right)
	: BinaryExpression(left, right) {
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
  return "(" + left_->alias() + " and " + right_->alias() + ")";
}

std::shared_ptr<Expression> normal::expression::gandiva::and_(const std::shared_ptr<Expression>& left,
															  const std::shared_ptr<Expression>& right) {
  return std::make_shared<And>(left, right);
}
