//
// Created by matt on 28/4/20.
//

#include "normal/expression/gandiva/Subtract.h"

using namespace normal::expression::gandiva;

Subtract::Subtract(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right) :
	left_(std::move(left)), right_(std::move(right)) {
}

void Subtract::compile(std::shared_ptr<arrow::Schema> schema) {
}

std::string Subtract::name() {
  return "subtract";
}

::gandiva::NodePtr Subtract::buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) {
  return ::gandiva::NodePtr();
}

std::shared_ptr<arrow::DataType> Subtract::resultType(std::shared_ptr<arrow::Schema> schema) {
  return std::shared_ptr<arrow::DataType>();
}

std::shared_ptr<Expression> normal::expression::gandiva::minus(std::shared_ptr<Expression> left,
															   std::shared_ptr<Expression> right) {
  return std::make_shared<Subtract>(std::move(left), std::move(right));
}
