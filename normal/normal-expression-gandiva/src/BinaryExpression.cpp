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

std::string BinaryExpression::genAliasForComparison(std::string compOp) {
  auto leftAlias = left_->alias();
  auto rightAlias = right_->alias();
  auto leftAlias_removePrefixInt = removePrefixInt(leftAlias);
  auto leftAlias_removePrefixFloat = removePrefixFloat(leftAlias);
  auto rightAlias_removePrefixInt = removePrefixInt(rightAlias);
  auto rightAlias_removePrefixFloat = removePrefixInt(rightAlias);
  if (leftAlias_removePrefixInt == nullptr && leftAlias_removePrefixFloat == nullptr) {
    if (rightAlias_removePrefixInt != nullptr) {
      return "cast(" + leftAlias + " as int) " + compOp + " " + *rightAlias_removePrefixInt;
    } else if(rightAlias_removePrefixFloat != nullptr) {
      return "cast(" + leftAlias + " as float) " + compOp + " " + *rightAlias_removePrefixFloat;
    } else {
      return leftAlias + " = " + rightAlias;
    }
  } else if (leftAlias_removePrefixInt != nullptr) {
    return *leftAlias_removePrefixInt + " " + compOp + " cast(" + rightAlias + " as int)";
  } else {
    return *leftAlias_removePrefixFloat + " " + compOp + " cast(" + rightAlias + " as float)";
  }
}
