//
// Created by matt on 11/6/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_GREATERTHANOREQUALTO_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_GREATERTHANOREQUALTO_H


#include <string>
#include <memory>

#include "Expression.h"
#include "BinaryExpression.h"

namespace fpdb::expression::gandiva {

class GreaterThanOrEqualTo : public BinaryExpression {

public:
  GreaterThanOrEqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);
  GreaterThanOrEqualTo() = default;
  GreaterThanOrEqualTo(const GreaterThanOrEqualTo&) = default;
  GreaterThanOrEqualTo& operator=(const GreaterThanOrEqualTo&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  static tl::expected<std::shared_ptr<GreaterThanOrEqualTo>, std::string> fromJson(const nlohmann::json &jObj);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, GreaterThanOrEqualTo& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("left", expr.left_),
                                 f.field("right", expr.right_));
  }
};

std::shared_ptr<Expression> gte(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_GREATERTHANOREQUALTO_H
