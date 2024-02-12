//
// Created by matt on 11/6/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_LESSTHANOREQUALTO_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_LESSTHANOREQUALTO_H


#include <string>
#include <memory>

#include "Expression.h"
#include "BinaryExpression.h"

namespace fpdb::expression::gandiva {

class LessThanOrEqualTo : public BinaryExpression {

public:
  LessThanOrEqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);
  LessThanOrEqualTo() = default;
  LessThanOrEqualTo(const LessThanOrEqualTo&) = default;
  LessThanOrEqualTo& operator=(const LessThanOrEqualTo&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  static tl::expected<std::shared_ptr<LessThanOrEqualTo>, std::string> fromJson(const nlohmann::json &jObj);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LessThanOrEqualTo& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("left", expr.left_),
                                 f.field("right", expr.right_));
  }
};

std::shared_ptr<Expression> lte(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right);

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_LESSTHANOREQUALTO_H
