//
// Created by matt on 28/4/20.
//

#include "normal/expression/gandiva/Divide.h"

using namespace normal::expression::gandiva;

Divide::Divide(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
	: left_(std::move(left)), right_(std::move(right)) {
}

void Divide::compile(std::shared_ptr<arrow::Schema> schema) {
}

std::string Divide::name() {
  return "divide";
}

::gandiva::NodePtr Divide::buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) {
  return ::gandiva::NodePtr();
}

std::shared_ptr<arrow::DataType> Divide::resultType(std::shared_ptr<arrow::Schema> schema) {
  return std::shared_ptr<arrow::DataType>();
}

std::shared_ptr<Expression> normal::expression::gandiva::divide(std::shared_ptr<Expression> left,
																std::shared_ptr<Expression> right) {
  return std::make_shared<Divide>(std::move(left), std::move(right));
}
