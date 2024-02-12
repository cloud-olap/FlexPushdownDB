//
// Created by Yifei Yang on 12/16/21.
//

#include <gandiva/tree_expr_builder.h>
#include <fpdb/expression/gandiva/DateExtract.h>
#include <fmt/format.h>
#include <sstream>

namespace fpdb::expression::gandiva {

DateExtract::DateExtract(const shared_ptr<Expression> &dateExpr, DateIntervalType intervalType):
  Expression(DATE_EXTRACT),
  dateExpr_(dateExpr), intervalType_(intervalType) {}

void DateExtract::compile(const shared_ptr<arrow::Schema> &schema) {
  dateExpr_->compile(schema);
  returnType_ = arrow::int64();

  string funcName = "extract" + intervalTypeToString(intervalType_);
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction(funcName,
                                                                {dateExpr_->getGandivaExpression()},
                                                                returnType_);
}

string DateExtract::alias() {
  return "?column?";
}

string DateExtract::getTypeString() const {
  return "DateExtract";
}

set<string> DateExtract::involvedColumnNames() {
  return dateExpr_->involvedColumnNames();
}

::nlohmann::json DateExtract::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("dateExpr", dateExpr_->toJson());
  jObj.emplace("intervalType", intervalTypeToString(intervalType_));
  return jObj;
}

tl::expected<std::shared_ptr<DateExtract>, std::string> DateExtract::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("dateExpr")) {
    return tl::make_unexpected(fmt::format("DateExpr not specified in DateExtract expression JSON '{}'", to_string(jObj)));
  }
  auto expDateExpr = Expression::fromJson(jObj["dateExpr"]);
  if (!expDateExpr) {
    return tl::make_unexpected(expDateExpr.error());
  }

  if (!jObj.contains("intervalType")) {
    return tl::make_unexpected(fmt::format("IntervalType not specified in DateExtract expression JSON '{}'", to_string(jObj)));
  }
  auto expIntervalType = stringToIntervalType(jObj["intervalType"]);
  if (!expIntervalType.has_value()) {
    return tl::make_unexpected(expIntervalType.error());
  }

  return std::make_shared<DateExtract>(*expDateExpr, *expIntervalType);
}

shared_ptr<Expression> dateExtract(const shared_ptr<Expression> &dateExpr, DateIntervalType intervalType) {
  return make_shared<DateExtract>(dateExpr, intervalType);
}

}
