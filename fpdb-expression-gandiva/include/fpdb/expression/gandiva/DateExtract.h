//
// Created by Yifei Yang on 12/16/21.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_DATEEXTRACT_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_DATEEXTRACT_H

#include "Expression.h"
#include "DateIntervalType.h"
#include <string>
#include <memory>

using namespace std;

namespace fpdb::expression::gandiva {

class DateExtract : public Expression {

public:
  DateExtract(const shared_ptr<Expression> &dateExpr, DateIntervalType intervalType);
  DateExtract() = default;
  DateExtract(const DateExtract&) = default;
  DateExtract& operator=(const DateExtract&) = default;

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() const override;
  set<string> involvedColumnNames() override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<DateExtract>, std::string> fromJson(const nlohmann::json &jObj);

private:
  shared_ptr<Expression> dateExpr_;
  DateIntervalType intervalType_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DateExtract& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("dateExpr", expr.dateExpr_),
                                 f.field("intervalType", expr.intervalType_));
  }
};

shared_ptr<Expression> dateExtract(const shared_ptr<Expression> &dateExpr, DateIntervalType intervalType);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_DATEEXTRACT_H
