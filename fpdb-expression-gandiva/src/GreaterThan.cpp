//
// Created by Yifei Yang on 7/14/20.
//

#include "fpdb/expression/gandiva/GreaterThan.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace fpdb::expression::gandiva;

GreaterThan::GreaterThan(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
  : BinaryExpression(std::move(Left), std::move(Right), GREATER_THAN) {}

void GreaterThan::compile(const std::shared_ptr<arrow::Schema> &schema) {
  left_->compile(schema);
  right_->compile(schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("greater_than",
                                                                {leftGandivaExpr, rightGandivaExpr},
                                                                returnType_);
}

std::string fpdb::expression::gandiva::GreaterThan::alias() {
  return genAliasForComparison(">");
}

std::string GreaterThan::getTypeString() const {
  return "GreaterThan";
}

tl::expected<std::shared_ptr<GreaterThan>, std::string> GreaterThan::fromJson(const nlohmann::json &jObj) {
  auto expOperands = BinaryExpression::fromJson(jObj);
  if (!expOperands.has_value()) {
    return tl::make_unexpected(expOperands.error());
  }
  return std::make_shared<GreaterThan>((*expOperands).first, (*expOperands).second);
}


std::shared_ptr<Expression> fpdb::expression::gandiva::gt(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right) {
  return std::make_shared<GreaterThan>(Left, Right);
}
