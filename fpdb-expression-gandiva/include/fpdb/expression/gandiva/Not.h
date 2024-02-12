//
// Created by Yifei Yang on 12/10/21.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_NOT_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_NOT_H

#include "Expression.h"
#include <memory>

using namespace std;

namespace fpdb::expression::gandiva {

class Not : public Expression {

public:
  Not(const shared_ptr<Expression> &expr);
  Not() = default;
  Not(const Not&) = default;
  Not& operator=(const Not&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  set<string> involvedColumnNames() override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<Not>, std::string> fromJson(const nlohmann::json &jObj);

private:
  shared_ptr<Expression> expr_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Not& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("expr", expr.expr_));
  }
};

shared_ptr<Expression> not_(const shared_ptr<Expression> &expr);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_NOT_H
