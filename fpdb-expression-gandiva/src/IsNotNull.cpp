//
// Created by Yifei Yang on 12/1/23.
//

#include <fpdb/expression/gandiva/IsNotNull.h>
#include <gandiva/tree_expr_builder.h>
#include <fmt/format.h>

namespace fpdb::expression::gandiva {

IsNotNull::IsNotNull(const std::shared_ptr<Expression> &expr):
  Expression(IS_NOT_NULL),
  expr_(expr) {}

void IsNotNull::compile(const std::shared_ptr<arrow::Schema> &schema) {
  expr_->compile(schema);
  returnType_ = ::arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("isnotnull",
                                                                {expr_->getGandivaExpression()},
                                                                returnType_);
}

std::string IsNotNull::alias() {
  return expr_->alias() + "is not null";
}

std::string IsNotNull::getTypeString() const {
  return "IsNotNull";
}

std::set<std::string> IsNotNull::involvedColumnNames() {
  return expr_->involvedColumnNames();
}

::nlohmann::json IsNotNull::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("expr", expr_->toJson());
  return jObj;
}

tl::expected<std::shared_ptr<IsNotNull>, std::string> IsNotNull::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("expr")) {
    return tl::make_unexpected(fmt::format("Expr not specified in IsNotNull expression JSON '{}'", to_string(jObj)));
  }
  auto expExpr = Expression::fromJson(jObj["expr"]);
  if (!expExpr) {
    return tl::make_unexpected(expExpr.error());
  }

  return std::make_shared<IsNotNull>(*expExpr);
}

bool IsNotNull::equalTo(const std::shared_ptr<Expression> &other) const {
  if (type_ != other->getType()) {
    return false;
  }
  auto typedOther = std::static_pointer_cast<IsNotNull>(other);
  return equals(expr_, typedOther->expr_);
}

std::shared_ptr<Expression> isNotNull(const std::shared_ptr<Expression> &expr) {
  return std::make_shared<IsNotNull>(expr);
}

}
