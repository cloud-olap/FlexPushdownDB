//
// Created by Yifei Yang on 12/10/21.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_DATEADD_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_DATEADD_H

#include "Expression.h"
#include "BinaryExpression.h"
#include "DateIntervalType.h"

using namespace std;

/**
 * Currently gandiva does not support date diff, so we transfer it to date add
 */
namespace fpdb::expression::gandiva {

class DateAdd : public BinaryExpression {

public:
  DateAdd(const shared_ptr<Expression>& left,
          const shared_ptr<Expression>& right,
          DateIntervalType intervalType);
  DateAdd() = default;
  DateAdd(const DateAdd&) = default;
  DateAdd& operator=(const DateAdd&) = default;

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() const override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<DateAdd>, std::string> fromJson(const nlohmann::json &jObj);

private:
  DateIntervalType intervalType_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DateAdd& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("left", expr.left_),
                                 f.field("right", expr.right_),
                                 f.field("intervalType", expr.intervalType_));
  }
};

shared_ptr<Expression> datePlus(const shared_ptr<Expression>& left,
                                const shared_ptr<Expression>& right,
                                DateIntervalType intervalType);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_DATEADD_H
