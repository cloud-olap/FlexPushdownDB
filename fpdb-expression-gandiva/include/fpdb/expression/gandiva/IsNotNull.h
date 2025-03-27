//
// Created by Yifei Yang on 12/1/23.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_ISNOTNULL_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_ISNOTNULL_H

#include "Expression.h"
#include <memory>

namespace fpdb::expression::gandiva {

class IsNotNull : public Expression {

public:
  IsNotNull(const std::shared_ptr<Expression> &expr);
  IsNotNull() = default;
  IsNotNull(const IsNotNull&) = default;
  IsNotNull& operator=(const IsNotNull&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  std::set<std::string> involvedColumnNames() override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<IsNotNull>, std::string> fromJson(const nlohmann::json &jObj);

private:
  bool equalTo(const std::shared_ptr<Expression> &other) const override;

  std::shared_ptr<Expression> expr_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, IsNotNull& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("expr", expr.expr_));
  }
};

std::shared_ptr<Expression> isNotNull(const std::shared_ptr<Expression> &expr);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_ISNOTNULL_H
