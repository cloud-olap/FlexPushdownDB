//
// Created by matt on 28/4/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_DIVIDE_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_DIVIDE_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"
#include "BinaryExpression.h"

namespace fpdb::expression::gandiva {

class Divide : public BinaryExpression {

public:
  Divide(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);
  Divide() = default;
  Divide(const Divide&) = default;
  Divide& operator=(const Divide&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  static tl::expected<std::shared_ptr<Divide>, std::string> fromJson(const nlohmann::json &jObj);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Divide& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("left", expr.left_),
                                 f.field("right", expr.right_));
  }
};

std::shared_ptr<Expression> divide(const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right);

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_DIVIDE_H
