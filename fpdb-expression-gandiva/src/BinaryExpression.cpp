//
// Created by Yifei Yang on 7/17/20.
//

#include "fpdb/expression/gandiva/BinaryExpression.h"
#include <fmt/format.h>
#include <utility>
#include <gandiva/tree_expr_builder.h>

using namespace fpdb::expression::gandiva;

BinaryExpression::BinaryExpression(std::shared_ptr<Expression> left,
                                   std::shared_ptr<Expression> right,
                                   ExpressionType type) :
  Expression(type),
  left_(std::move(left)),
  right_(std::move(right)) {}

std::set<std::string> BinaryExpression::involvedColumnNames() {
  auto leftInvolvedColumnNames = getLeft()->involvedColumnNames();
  auto rightInvolvedColumnNames = getRight()->involvedColumnNames();
  leftInvolvedColumnNames.insert(rightInvolvedColumnNames.begin(), rightInvolvedColumnNames.end());
  return leftInvolvedColumnNames;
}

::nlohmann::json BinaryExpression::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("left", left_->toJson());
  jObj.emplace("right", right_->toJson());
  return jObj;
}

tl::expected<std::pair<std::shared_ptr<Expression>, std::shared_ptr<Expression>>, std::string>
BinaryExpression::fromJson(const nlohmann::json &jObj) {
  auto type = jObj["type"].get<std::string>();

  if (!jObj.contains("left")) {
    return tl::make_unexpected(fmt::format("Left expression not specified in '{}' expression JSON '{}'", type, to_string(jObj)));
  }
  auto expLeft = Expression::fromJson(jObj["left"]);
  if (!expLeft.has_value()) {
    return tl::make_unexpected(expLeft.error());
  }

  if (!jObj.contains("right")) {
    return tl::make_unexpected(fmt::format("Right expression not specified in '{}' expression JSON '{}'", type, to_string(jObj)));
  }
  auto expRight = Expression::fromJson(jObj["right"]);
  if (!expRight.has_value()) {
    return tl::make_unexpected(expRight.error());
  }

  return std::make_pair(*expLeft, *expRight);
}

std::tuple<shared_ptr<arrow::DataType>, ::gandiva::NodePtr, ::gandiva::NodePtr>
BinaryExpression::castGandivaExprToUpperType() {
  // int32 vs int64
  if (left_->getReturnType()->id() == arrow::Type::INT32 && right_->getReturnType()->id() == arrow::Type::INT64) {
    auto upperType = arrow::int64();
    return make_tuple(upperType,
                      ::gandiva::TreeExprBuilder::MakeFunction("castBIGINT", {left_->getGandivaExpression()}, upperType),
                      right_->getGandivaExpression());
  } else if (right_->getReturnType()->id() == arrow::Type::INT32 && left_->getReturnType()->id() == arrow::Type::INT64) {
    auto upperType = arrow::int64();
    return make_tuple(upperType,
                      left_->getGandivaExpression(),
                      ::gandiva::TreeExprBuilder::MakeFunction("castBIGINT", {right_->getGandivaExpression()}, upperType));
  }

  // int64 vs double
  else if (left_->getReturnType()->id() == arrow::Type::INT64 && right_->getReturnType()->id() == arrow::Type::DOUBLE) {
    auto upperType = arrow::float64();
    return make_tuple(upperType,
                      ::gandiva::TreeExprBuilder::MakeFunction("castFLOAT8", {left_->getGandivaExpression()}, upperType),
                      right_->getGandivaExpression());
  } else if (right_->getReturnType()->id() == arrow::Type::INT64 && left_->getReturnType()->id() == arrow::Type::DOUBLE) {
    auto upperType = arrow::float64();
    return make_tuple(upperType,
                      left_->getGandivaExpression(),
                      ::gandiva::TreeExprBuilder::MakeFunction("castFLOAT8", {right_->getGandivaExpression()}, upperType));
  }

  // int32 vs double
  else if (left_->getReturnType()->id() == arrow::Type::INT32 && right_->getReturnType()->id() == arrow::Type::DOUBLE) {
    auto upperType = arrow::float64();
    return make_tuple(upperType,
                      ::gandiva::TreeExprBuilder::MakeFunction("castFLOAT8", {left_->getGandivaExpression()}, upperType),
                      right_->getGandivaExpression());
  } else if (right_->getReturnType()->id() == arrow::Type::INT32 && left_->getReturnType()->id() == arrow::Type::DOUBLE) {
    auto upperType = arrow::float64();
    return make_tuple(upperType,
                      left_->getGandivaExpression(),
                      ::gandiva::TreeExprBuilder::MakeFunction("castFLOAT8", {right_->getGandivaExpression()}, upperType));
  }

  // otherwise, nothing changed
  return make_tuple(left_->getReturnType(),
                    left_->getGandivaExpression(),
                    right_->getGandivaExpression());
}


const std::shared_ptr<Expression> &BinaryExpression::getLeft() const {
  return left_;
}

const std::shared_ptr<Expression> &BinaryExpression::getRight() const {
  return right_;
}

std::string BinaryExpression::genAliasForComparison(const std::string& compOp) {
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
      return leftAlias + compOp + rightAlias;
    }
  } else if (leftAlias_removePrefixInt != nullptr) {
    return *leftAlias_removePrefixInt + " " + compOp + " cast(" + rightAlias + " as int)";
  } else {
    return *leftAlias_removePrefixFloat + " " + compOp + " cast(" + rightAlias + " as float)";
  }
}

[[maybe_unused]] void BinaryExpression::setLeft(const std::shared_ptr<Expression> &left) {
  left_ = left;
}

[[maybe_unused]] void BinaryExpression::setRight(const std::shared_ptr<Expression> &right) {
  right_ = right;
}
