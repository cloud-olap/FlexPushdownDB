//
// Created by matt on 11/6/20.
//

#include "fpdb/expression/gandiva/EqualTo.h"
#include <utility>
#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace fpdb::expression::gandiva;

EqualTo::EqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
	: BinaryExpression(std::move(Left), std::move(Right), EQUAL_TO) {}

void EqualTo::compile(const std::shared_ptr<arrow::Schema> &Schema) {
  left_->compile(Schema);
  right_->compile(Schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("equal",
                                                                {leftGandivaExpr, rightGandivaExpr},
                                                                returnType_);
}

std::string EqualTo::alias() {
  return genAliasForComparison("=");
}

std::string EqualTo::getTypeString() const {
  return "EqualTo";
}

tl::expected<std::shared_ptr<EqualTo>, std::string> EqualTo::fromJson(const nlohmann::json &jObj) {
  auto expOperands = BinaryExpression::fromJson(jObj);
  if (!expOperands.has_value()) {
    return tl::make_unexpected(expOperands.error());
  }
  return std::make_shared<EqualTo>((*expOperands).first, (*expOperands).second);
}

std::shared_ptr<Expression> fpdb::expression::gandiva::eq(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right) {
  return std::make_shared<EqualTo>(Left, Right);
}
