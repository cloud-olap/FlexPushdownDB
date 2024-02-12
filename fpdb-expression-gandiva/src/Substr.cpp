//
// Created by Yifei Yang on 1/8/22.
//

#include <fpdb/expression/gandiva/Substr.h>
#include <gandiva/tree_expr_builder.h>
#include <fmt/format.h>

namespace fpdb::expression::gandiva {

Substr::Substr(const shared_ptr<Expression> &expr,
               const shared_ptr<Expression> &fromLit,
               const shared_ptr<Expression> &forLit) :
  Expression(SUBSTR),
  expr_(expr),
  fromLit_(fromLit),
  forLit_(forLit) {}

void Substr::compile(const shared_ptr<arrow::Schema> &schema) {
  expr_->compile(schema);
  fromLit_->compile(schema);
  forLit_->compile(schema);

  returnType_ = arrow::utf8();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction(
          "substr",
          {expr_->getGandivaExpression(), fromLit_->getGandivaExpression(), forLit_->getGandivaExpression()},
          returnType_);
}

string Substr::alias() {
  return "?column?";
}

string Substr::getTypeString() const {
  return "Substr";
}

set<string> Substr::involvedColumnNames() {
  auto involvedColumnNames = expr_->involvedColumnNames();
  const auto &fromInvolvedColumnNames = fromLit_->involvedColumnNames();
  const auto &forInvolvedColumnNames = forLit_->involvedColumnNames();
  involvedColumnNames.insert(fromInvolvedColumnNames.begin(), fromInvolvedColumnNames.end());
  involvedColumnNames.insert(forInvolvedColumnNames.begin(), forInvolvedColumnNames.end());
  return involvedColumnNames;
}

::nlohmann::json Substr::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("expr", expr_->toJson());
  jObj.emplace("fromLit", fromLit_->toJson());
  jObj.emplace("forLit", forLit_->toJson());
  return jObj;
}

tl::expected<std::shared_ptr<Substr>, std::string> Substr::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("expr")) {
    return tl::make_unexpected(fmt::format("Expr not specified in Substr expression JSON '{}'", to_string(jObj)));
  }
  auto expExpr = Expression::fromJson(jObj["expr"]);
  if (!expExpr) {
    return tl::make_unexpected(expExpr.error());
  }

  if (!jObj.contains("fromLit")) {
    return tl::make_unexpected(fmt::format("FromLit not specified in Substr expression JSON '{}'", to_string(jObj)));
  }
  auto expFromLit = Expression::fromJson(jObj["fromLit"]);
  if (!expFromLit) {
    return tl::make_unexpected(expFromLit.error());
  }

  if (!jObj.contains("forLit")) {
    return tl::make_unexpected(fmt::format("ForLit not specified in Substr expression JSON '{}'", to_string(jObj)));
  }
  auto expForLit = Expression::fromJson(jObj["forLit"]);
  if (!expForLit) {
    return tl::make_unexpected(expForLit.error());
  }

  return std::make_shared<Substr>(*expExpr, *expFromLit, *expForLit);
}

shared_ptr<Expression> substr(const shared_ptr<Expression> &expr,
                              const shared_ptr<Expression> &fromLit,
                              const shared_ptr<Expression> &forLit) {
  return make_shared<Substr>(expr, fromLit, forLit);
}

}
