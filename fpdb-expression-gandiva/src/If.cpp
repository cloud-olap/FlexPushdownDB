//
// Created by Yifei Yang on 12/10/21.
//

#include <fpdb/expression/gandiva/If.h>
#include <fpdb/expression/gandiva/Cast.h>
#include <gandiva/tree_expr_builder.h>
#include <fmt/format.h>

namespace fpdb::expression::gandiva {

If::If(const shared_ptr<Expression> &ifExpr,
       const shared_ptr<Expression> &thenExpr,
       const shared_ptr<Expression> &elseExpr) :
  Expression(IF),
  ifExpr_(ifExpr),
  thenExpr_(thenExpr),
  elseExpr_(elseExpr) {}

void If::compile(const shared_ptr<arrow::Schema> &schema) {
  ifExpr_->compile(schema);
  thenExpr_->compile(schema);
  elseExpr_->compile(schema);

  // check return type between "then" and "else" and cast if needed
  const auto &thenType = thenExpr_->getReturnType();
  const auto &elseType = elseExpr_->getReturnType();
  if (thenType->id() == elseType->id()) {
    returnType_ = thenExpr_->getReturnType();
  } else {
    if (Cast::getCastDirection(thenType, elseType)) {
      thenExpr_ = cast(thenExpr_, elseType);
      thenExpr_->compile(schema);
      returnType_ = elseExpr_->getReturnType();
    } else {
      elseExpr_ = cast(elseExpr_, thenType);
      elseExpr_->compile(schema);
      returnType_ = thenExpr_->getReturnType();
    }
  }

  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeIf(ifExpr_->getGandivaExpression(),
                                                          thenExpr_->getGandivaExpression(),
                                                          elseExpr_->getGandivaExpression(),
                                                          returnType_);
}

string If::alias() {
  return "?column?";
}

string If::getTypeString() const {
  return "If";
}

set<string> If::involvedColumnNames() {
  auto ifInvolvedColumnNames = ifExpr_->involvedColumnNames();
  const auto &thenInvolvedColumnNames = thenExpr_->involvedColumnNames();
  const auto &elseInvolvedColumnNames = elseExpr_->involvedColumnNames();
  ifInvolvedColumnNames.insert(thenInvolvedColumnNames.begin(), thenInvolvedColumnNames.end());
  ifInvolvedColumnNames.insert(elseInvolvedColumnNames.begin(), elseInvolvedColumnNames.end());
  return ifInvolvedColumnNames;
}

::nlohmann::json If::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("ifExpr", ifExpr_->toJson());
  jObj.emplace("thenExpr", thenExpr_->toJson());
  jObj.emplace("elseExpr", elseExpr_->toJson());
  return jObj;
}

tl::expected<std::shared_ptr<If>, std::string> If::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("ifExpr")) {
    return tl::make_unexpected(fmt::format("IfExpr not specified in If expression JSON '{}'", to_string(jObj)));
  }
  auto expIfExpr = Expression::fromJson(jObj["ifExpr"]);
  if (!expIfExpr) {
    return tl::make_unexpected(expIfExpr.error());
  }

  if (!jObj.contains("thenExpr")) {
    return tl::make_unexpected(fmt::format("ThenExpr not specified in If expression JSON '{}'", to_string(jObj)));
  }
  auto expThenExpr = Expression::fromJson(jObj["thenExpr"]);
  if (!expThenExpr) {
    return tl::make_unexpected(expThenExpr.error());
  }

  if (!jObj.contains("elseExpr")) {
    return tl::make_unexpected(fmt::format("ElseExpr not specified in If expression JSON '{}'", to_string(jObj)));
  }
  auto expElseExpr = Expression::fromJson(jObj["elseExpr"]);
  if (!expElseExpr) {
    return tl::make_unexpected(expElseExpr.error());
  }

  return std::make_shared<If>(*expIfExpr, *expThenExpr, *expElseExpr);
}

bool If::equalTo(const std::shared_ptr<Expression> &other) const {
  if (type_ != other->getType()) {
    return false;
  }
  auto typedOther = std::static_pointer_cast<If>(other);
  return equals(ifExpr_, typedOther->ifExpr_) && equals(thenExpr_, typedOther->thenExpr_) &&
         equals(elseExpr_, typedOther->elseExpr_);
}

shared_ptr<Expression> if_(const shared_ptr<Expression> &ifExpr,
                           const shared_ptr<Expression> &thenExpr,
                           const shared_ptr<Expression> &elseExpr) {
  return make_shared<If>(ifExpr, thenExpr, elseExpr);
}

}
