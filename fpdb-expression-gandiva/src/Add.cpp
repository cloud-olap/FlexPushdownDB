//
// Created by matt on 28/4/20.
//

#include <gandiva/tree_expr_builder.h>
#include "fpdb/expression/gandiva/Add.h"

using namespace fpdb::expression::gandiva;

Add::Add(const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right)
	: BinaryExpression(left, right, ADD) {
}

void Add::compile(const std::shared_ptr<arrow::Schema> &schema) {
  left_->compile(schema);
  right_->compile(schema);

  const auto &castRes = castGandivaExprToUpperType();
  const auto &leftGandivaExpr = get<1>(castRes);
  const auto &rightGandivaExpr = get<2>(castRes);

  returnType_ = get<0>(castRes);
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("add",
                                                                {leftGandivaExpr, rightGandivaExpr},
                                                                returnType_);
}

std::string Add::alias() {
  return "?column?";
}

std::string Add::getTypeString() const {
  return "Add";
}

tl::expected<std::shared_ptr<Add>, std::string> Add::fromJson(const nlohmann::json &jObj) {
  auto expOperands = BinaryExpression::fromJson(jObj);
  if (!expOperands.has_value()) {
    return tl::make_unexpected(expOperands.error());
  }
  return std::make_shared<Add>((*expOperands).first, (*expOperands).second);
}

std::shared_ptr<Expression> fpdb::expression::gandiva::plus(const std::shared_ptr<Expression>& left,
															  const std::shared_ptr<Expression>& right) {
  return std::make_shared<Add>(left, right);
}
