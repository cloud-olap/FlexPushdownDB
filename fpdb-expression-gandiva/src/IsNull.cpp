//
// Created by Yifei Yang on 1/7/22.
//

#include <fpdb/expression/gandiva/IsNull.h>
#include <gandiva/tree_expr_builder.h>
#include <fmt/format.h>

namespace fpdb::expression::gandiva {

IsNull::IsNull(const shared_ptr<Expression> &expr):
  Expression(IS_NULL),
  expr_(expr) {}

void IsNull::compile(const shared_ptr<arrow::Schema> &schema) {
  expr_->compile(schema);
  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("isnull",
                                                                {expr_->getGandivaExpression()},
                                                                returnType_);
}

string IsNull::alias() {
  return expr_->alias() + "is null";
}

string IsNull::getTypeString() const {
  return "IsNull";
}

set<string> IsNull::involvedColumnNames() {
  return expr_->involvedColumnNames();
}

::nlohmann::json IsNull::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("expr", expr_->toJson());
  return jObj;
}

tl::expected<std::shared_ptr<IsNull>, std::string> IsNull::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("expr")) {
    return tl::make_unexpected(fmt::format("Expr not specified in IsNull expression JSON '{}'", to_string(jObj)));
  }
  auto expExpr = Expression::fromJson(jObj["expr"]);
  if (!expExpr) {
    return tl::make_unexpected(expExpr.error());
  }

  return std::make_shared<IsNull>(*expExpr);
}

shared_ptr<Expression> isNull(const shared_ptr<Expression> &expr) {
  return make_shared<IsNull>(expr);
}

}
