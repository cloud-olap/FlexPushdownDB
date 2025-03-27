//
// Created by Yifei Yang on 12/11/24.
//

#include <fpdb/expression/gandiva/Concat.h>
#include <gandiva/tree_expr_builder.h>
#include <fmt/format.h>

namespace fpdb::expression::gandiva {

Concat::Concat(const std::vector<std::shared_ptr<Expression>> &exprs) :
  Expression(CONCAT),
  exprs_(exprs) {}

void Concat::compile(const std::shared_ptr<arrow::Schema> &schema) {
  ::gandiva::NodeVector gandivaExprs;
  for (const auto &expr: exprs_) {
    expr->compile(schema);
    gandivaExprs.emplace_back(expr->getGandivaExpression());
  }

  returnType_ = exprs_[0]->getReturnType();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction(
          "concatOperator", gandivaExprs, returnType_);
}

std::string Concat::alias() {
  return "?column?";
}

std::string Concat::getTypeString() const {
  return "Concat";
}

std::set<std::string> Concat::involvedColumnNames() {
  std::set<std::string> allInvolvedColumnNames;
  for (const auto &expr: exprs_) {
    const auto &involvedColumnNames = expr->involvedColumnNames();
    allInvolvedColumnNames.insert(involvedColumnNames.begin(), involvedColumnNames.end());
  }
  return allInvolvedColumnNames;
}

::nlohmann::json Concat::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());

  std::vector<::nlohmann::json> operandsJArr;
  for (const auto &expr: exprs_) {
    operandsJArr.emplace_back(expr->toJson());
  }
  jObj.emplace("exprs", operandsJArr);

  return jObj;
}

tl::expected<std::shared_ptr<Concat>, std::string> Concat::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("exprs")) {
    return tl::make_unexpected(fmt::format("Exprs not specified in Concat expression JSON '{}'", to_string(jObj)));
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

  return std::make_shared<Concat>(exprs);
}

bool Concat::equalTo(const std::shared_ptr<Expression> &other) const {
  if (type_ != other->getType()) {
    return false;
  }
  auto typedOther = std::static_pointer_cast<Concat>(other);
  return equals(exprs_, typedOther->exprs_);
}

std::shared_ptr<Expression> concat(const std::vector<std::shared_ptr<Expression>> &exprs) {
  return std::make_shared<Concat>(exprs);
}

}
