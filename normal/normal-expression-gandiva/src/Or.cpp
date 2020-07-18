//
// Created by Yifei Yang on 7/15/20.
//

#include <gandiva/tree_expr_builder.h>
#include "normal/expression/gandiva/Or.h"

using namespace normal::expression::gandiva;

Or::Or(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
        : BinaryExpression(left, right) {
}

void Or::compile(std::shared_ptr<arrow::Schema> schema) {

  left_->compile(schema);
  right_->compile(schema);

  auto leftGandivaExpression = left_->getGandivaExpression();
  auto rightGandivaExpression = right_->getGandivaExpression();

  auto orExpression = ::gandiva::TreeExprBuilder::MakeOr(
          {leftGandivaExpression, rightGandivaExpression});

  gandivaExpression_ = orExpression;
  returnType_ = ::arrow::boolean();
}

std::string Or::alias() {
  return "?column?";
}

std::shared_ptr<Expression> normal::expression::gandiva::or_(std::shared_ptr<Expression> left,
                                                              std::shared_ptr<Expression> right) {
  return std::make_shared<Or>(std::move(left), std::move(right));
}
