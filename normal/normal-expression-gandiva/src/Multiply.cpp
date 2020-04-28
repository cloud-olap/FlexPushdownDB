//
// Created by matt on 28/4/20.
//

#include "normal/expression/gandiva/Multiply.h"

using namespace normal::expression::gandiva;

Multiply::Multiply(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
	: left_(std::move(left)), right_(std::move(right)) {
}

void Multiply::compile(std::shared_ptr<arrow::Schema> schema) {
}

std::string Multiply::alias() {
  return "multiply";
}

std::shared_ptr<Expression> normal::expression::gandiva::times(std::shared_ptr<Expression> left,
															   std::shared_ptr<Expression> right) {
  return std::make_shared<Multiply>(std::move(left), std::move(right));
}
