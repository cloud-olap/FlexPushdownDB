//
// Created by Yifei Yang on 7/15/20.
//

#include <fpdb/expression/gandiva/Or.h>
#include <gandiva/tree_expr_builder.h>
#include <fmt/format.h>
#include <sstream>

using namespace fpdb::expression::gandiva;

Or::Or(const vector<shared_ptr<Expression>>& exprs):
  Expression(OR),
  exprs_(exprs) {}

void Or::compile(const shared_ptr<arrow::Schema> &schema) {
  ::gandiva::NodeVector gandivaExprs;
  for (const auto &expr: exprs_) {
    expr->compile(schema);
    gandivaExprs.emplace_back(expr->getGandivaExpression());
  }

  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeOr(gandivaExprs);
}

string Or::alias() {
  stringstream ss;
  ss << "(";
  for (uint i = 0; i < exprs_.size(); ++i) {
    ss << exprs_[i]->alias();
    if (i < exprs_.size() - 1) {
      ss << " or ";
    }
  }
  ss << ")";
  return ss.str();
}

string Or::getTypeString() const {
  return "Or";
}

set<string> Or::involvedColumnNames() {
  set<string> allInvolvedColumnNames;
  for (const auto &expr: exprs_) {
    const auto &involvedColumnNames = expr->involvedColumnNames();
    allInvolvedColumnNames.insert(involvedColumnNames.begin(), involvedColumnNames.end());
  }
  return allInvolvedColumnNames;
}

::nlohmann::json Or::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());

  vector<::nlohmann::json> operandsJArr;
  for (const auto &expr: exprs_) {
    operandsJArr.emplace_back(expr->toJson());
  }
  jObj.emplace("exprs", operandsJArr);

  return jObj;
}

tl::expected<std::shared_ptr<Or>, std::string> Or::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("exprs")) {
    return tl::make_unexpected(fmt::format("Exprs not specified in Or expression JSON '{}'", to_string(jObj)));
  }
  auto exprsJArr = jObj["exprs"].get<std::vector<nlohmann::json>>();

  std::vector<std::shared_ptr<Expression>> exprs;
  for (const auto &exprJObj: exprsJArr) {
    auto expExpr = Expression::fromJson(exprJObj);
    if (!expExpr.has_value()) {
      return tl::make_unexpected(expExpr.error());
    }
    exprs.emplace_back(*expExpr);
  }

  return std::make_shared<Or>(exprs);
}

const vector<shared_ptr<Expression>> &Or::getExprs() const {
  return exprs_;
}

shared_ptr<Expression> fpdb::expression::gandiva::or_(const shared_ptr<Expression>& left,
                                                              const shared_ptr<Expression>& right) {
  const vector<shared_ptr<Expression>> exprs{left, right};
  return make_shared<Or>(exprs);
}

shared_ptr<Expression>
fpdb::expression::gandiva::or_(const vector<shared_ptr<Expression>> &exprs) {
  return make_shared<Or>(exprs);
}
