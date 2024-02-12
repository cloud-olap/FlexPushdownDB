//
// Created by matt on 6/5/20.
//

#include "fpdb/expression/gandiva/LessThan.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace fpdb::expression::gandiva;

LessThan::LessThan(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
	: BinaryExpression(std::move(Left), std::move(Right), LESS_THAN) {}

void LessThan::compile(const std::shared_ptr<arrow::Schema> &Schema) {
  left_->compile(Schema);
  right_->compile(Schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("less_than",
                                                                {leftGandivaExpr, rightGandivaExpr},
                                                                returnType_);
}

std::string LessThan::alias() {
  return genAliasForComparison("<");
}

std::string LessThan::getTypeString() const {
  return "LessThan";
}

tl::expected<std::shared_ptr<LessThan>, std::string> LessThan::fromJson(const nlohmann::json &jObj) {
  auto expOperands = BinaryExpression::fromJson(jObj);
  if (!expOperands.has_value()) {
    return tl::make_unexpected(expOperands.error());
  }
  return std::make_shared<LessThan>((*expOperands).first, (*expOperands).second);
}

std::shared_ptr<Expression> fpdb::expression::gandiva::lt(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right) {
  return std::make_shared<LessThan>(Left, Right);
}
