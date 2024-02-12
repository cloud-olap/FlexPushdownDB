//
// Created by Yifei Yang on 12/10/21.
//

#include <gandiva/tree_expr_builder.h>
#include <fpdb/expression/gandiva/DateAdd.h>
#include <fmt/format.h>
#include <sstream>

namespace fpdb::expression::gandiva {

DateAdd::DateAdd(const shared_ptr<Expression>& left, 
                 const shared_ptr<Expression>& right,
                 DateIntervalType intervalType):
  BinaryExpression(left, right, DATE_ADD),
  intervalType_(intervalType) {
}

void DateAdd::compile(const shared_ptr<arrow::Schema> &schema) {
  left_->compile(schema);
  right_->compile(schema);

  const auto &leftGandivaExpr = left_->getGandivaExpression();
  const auto &rightGandivaExpr = right_->getGandivaExpression();
  returnType_ = arrow::date64();

  string funcName = "timestampadd" + intervalTypeToString(intervalType_);
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction(funcName,
                                                                {leftGandivaExpr, rightGandivaExpr},
                                                                returnType_);
}

string DateAdd::alias() {
  return "?column?";
}

string DateAdd::getTypeString() const {
  return "DateAdd";
}

::nlohmann::json DateAdd::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("left", left_->toJson());
  jObj.emplace("right", right_->toJson());
  jObj.emplace("intervalType", intervalTypeToString(intervalType_));
  return jObj;
}

tl::expected<std::shared_ptr<DateAdd>, std::string> DateAdd::fromJson(const nlohmann::json &jObj) {
  auto expOperands = BinaryExpression::fromJson(jObj);
  if (!expOperands.has_value()) {
    return tl::make_unexpected(expOperands.error());
  }

  if (!jObj.contains("intervalType")) {
    return tl::make_unexpected(fmt::format("IntervalType not specified in DateAdd expression JSON '{}'", to_string(jObj)));
  }
  auto expIntervalType = stringToIntervalType(jObj["intervalType"]);
  if (!expIntervalType.has_value()) {
    return tl::make_unexpected(expIntervalType.error());
  }

  return std::make_shared<DateAdd>(expOperands->first, expOperands->second, *expIntervalType);
}

shared_ptr<Expression> datePlus(const shared_ptr<Expression>& left,
                                const shared_ptr<Expression>& right,
                                DateIntervalType intervalType) {
  return make_shared<DateAdd>(left, right, intervalType);
}

}
