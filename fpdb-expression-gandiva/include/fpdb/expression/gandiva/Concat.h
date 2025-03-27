//
// Created by Yifei Yang on 12/11/24.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_CONCAT_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_CONCAT_H

#include "Expression.h"
#include <memory>

namespace fpdb::expression::gandiva {

class Concat : public Expression {
public:
  Concat(const std::vector<std::shared_ptr<Expression>> &exprs);
  Concat() = default;
  Concat(const Concat&) = default;
  Concat& operator=(const Concat&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  std::set<std::string> involvedColumnNames() override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<Concat>, std::string> fromJson(const nlohmann::json &jObj);

private:
  bool equalTo(const std::shared_ptr<Expression> &other) const override;

  std::vector<std::shared_ptr<Expression>> exprs_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Concat& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("exprs", expr.exprs_));
  }
};

std::shared_ptr<Expression> concat(const std::vector<std::shared_ptr<Expression>> &exprs);

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_CONCAT_H
