//
// Created by Yifei Yang on 1/6/22.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_NOTEQUALTO_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_NOTEQUALTO_H

#include <string>
#include <memory>

#include "Expression.h"
#include "BinaryExpression.h"

namespace fpdb::expression::gandiva {

class NotEqualTo : public BinaryExpression {

public:
  NotEqualTo(shared_ptr<Expression> Left, shared_ptr<Expression> Right);
  NotEqualTo() = default;
  NotEqualTo(const NotEqualTo&) = default;
  NotEqualTo& operator=(const NotEqualTo&) = default;

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() const override;
  static tl::expected<std::shared_ptr<NotEqualTo>, std::string> fromJson(const nlohmann::json &jObj);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, NotEqualTo& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("left", expr.left_),
                                 f.field("right", expr.right_));
  }
};

shared_ptr<Expression> neq(const shared_ptr<Expression>& left, const shared_ptr<Expression>& right);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_NOTEQUALTO_H
