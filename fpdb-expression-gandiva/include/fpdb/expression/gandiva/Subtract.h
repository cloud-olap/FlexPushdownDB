//
// Created by matt on 28/4/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_SUBTRACT_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_SUBTRACT_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"
#include "BinaryExpression.h"

namespace fpdb::expression::gandiva {

class Subtract : public BinaryExpression {

public:
  Subtract(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);
  Subtract() = default;
  Subtract(const Subtract&) = default;
  Subtract& operator=(const Subtract&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  static tl::expected<std::shared_ptr<Subtract>, std::string> fromJson(const nlohmann::json &jObj);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Subtract& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("left", expr.left_),
                                 f.field("right", expr.right_));
  }
};

std::shared_ptr<Expression> minus(const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right);

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_SUBTRACT_H
