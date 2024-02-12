//
// Created by Yifei Yang on 12/10/21.
//

#include <fpdb/expression/gandiva/Not.h>
#include <gandiva/tree_expr_builder.h>
#include <fmt/format.h>

namespace fpdb::expression::gandiva {

Not::Not(const shared_ptr<Expression> &expr):
  Expression(NOT),
  expr_(expr) {}

void Not::compile(const shared_ptr<arrow::Schema> &schema) {
  expr_->compile(schema);
  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("not",
                                                                {expr_->getGandivaExpression()},
                                                                returnType_);
}

string Not::alias() {
  return "not (" + expr_->alias() + ")";
}

string Not::getTypeString() const {
  return "Not";
}

set<string> Not::involvedColumnNames() {
  return expr_->involvedColumnNames();
}

::nlohmann::json Not::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("expr", expr_->toJson());
  return jObj;
}

tl::expected<std::shared_ptr<Not>, std::string> Not::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("expr")) {
    return tl::make_unexpected(fmt::format("Expr not specified in Not expression JSON '{}'", to_string(jObj)));
  }
  auto expExpr = Expression::fromJson(jObj["expr"]);
  if (!expExpr) {
    return tl::make_unexpected(expExpr.error());
  }

  return std::make_shared<Not>(*expExpr);
}

shared_ptr<Expression> not_(const shared_ptr<Expression> &expr) {
  return make_shared<Not>(expr);
}

}
