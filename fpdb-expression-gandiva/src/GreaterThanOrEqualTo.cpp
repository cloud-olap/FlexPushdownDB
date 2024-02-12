//
// Created by matt on 11/6/20.
//

#include "fpdb/expression/gandiva/GreaterThanOrEqualTo.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace fpdb::expression::gandiva;

GreaterThanOrEqualTo::GreaterThanOrEqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
	: BinaryExpression(std::move(Left), std::move(Right), GREATER_THAN_OR_EQUAL_TO) {}

void GreaterThanOrEqualTo::compile(const std::shared_ptr<arrow::Schema> &Schema) {
  left_->compile(Schema);
  right_->compile(Schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("greater_than_or_equal_to",
                                                                {leftGandivaExpr, rightGandivaExpr},
                                                                returnType_);
}

std::string GreaterThanOrEqualTo::alias() {
  return genAliasForComparison(">=");
}

std::string GreaterThanOrEqualTo::getTypeString() const {
  return "GreaterThanOrEqualTo";
}

tl::expected<std::shared_ptr<GreaterThanOrEqualTo>, std::string>
GreaterThanOrEqualTo::fromJson(const nlohmann::json &jObj) {
  auto expOperands = BinaryExpression::fromJson(jObj);
  if (!expOperands.has_value()) {
    return tl::make_unexpected(expOperands.error());
  }
  return std::make_shared<GreaterThanOrEqualTo>((*expOperands).first, (*expOperands).second);
}

std::shared_ptr<Expression> fpdb::expression::gandiva::gte(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right) {
  return std::make_shared<GreaterThanOrEqualTo>(Left, Right);
}
