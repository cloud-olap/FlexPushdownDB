//
// Created by Yifei Yang on 7/17/20.
//

#include "normal/expression/gandiva/BinaryExpression.h"

using namespace normal::expression::gandiva;

BinaryExpression::BinaryExpression(const std::shared_ptr<Expression> &left, const std::shared_ptr<Expression> &right)
        : left_(std::move(left)), right_(std::move(right)) {}

const std::shared_ptr<Expression> &BinaryExpression::getLeft() const {
  return left_;
}

const std::shared_ptr<Expression> &BinaryExpression::getRight() const {
  return right_;
}

void BinaryExpression::compile(std::shared_ptr<arrow::Schema> schema) {

}

std::string BinaryExpression::alias() {
  return std::string();
}
