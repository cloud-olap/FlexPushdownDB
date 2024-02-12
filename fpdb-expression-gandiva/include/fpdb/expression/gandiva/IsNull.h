//
// Created by Yifei Yang on 1/7/22.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_ISNULL_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_ISNULL_H

#include "Expression.h"
#include <memory>

using namespace std;

namespace fpdb::expression::gandiva {

class IsNull : public Expression {

public:
  IsNull(const shared_ptr<Expression> &expr);
  IsNull() = default;
  IsNull(const IsNull&) = default;
  IsNull& operator=(const IsNull&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  set<string> involvedColumnNames() override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<IsNull>, std::string> fromJson(const nlohmann::json &jObj);

private:
  shared_ptr<Expression> expr_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, IsNull& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("expr", expr.expr_));
  }
};

shared_ptr<Expression> isNull(const shared_ptr<Expression> &expr);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_ISNULL_H
