//
// Created by matt on 28/4/20.
//

#include "normal/expression/gandiva/Subtract.h"

using namespace normal::expression::gandiva;

Subtract::Subtract(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right) :
	BinaryExpression(left, right) {
}

void Subtract::compile(std::shared_ptr<arrow::Schema> /* schema */) {
  // TODO
}

std::string Subtract::alias() {
  return "subtract";
}

std::shared_ptr<Expression> normal::expression::gandiva::minus(std::shared_ptr<Expression> left,
															   std::shared_ptr<Expression> right) {
  return std::make_shared<Subtract>(std::move(left), std::move(right));
}
