//
// Created by Yifei Yang on 1/8/22.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_SUBSTR_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_SUBSTR_H

#include "Expression.h"
#include <memory>

using namespace std;

namespace fpdb::expression::gandiva {

class Substr : public Expression {

public:
  Substr(const shared_ptr<Expression> &expr,
         const shared_ptr<Expression> &fromLit,
         const shared_ptr<Expression> &forLit);
  Substr() = default;
  Substr(const Substr&) = default;
  Substr& operator=(const Substr&) = default;

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() const override;
  set<string> involvedColumnNames() override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<Substr>, std::string> fromJson(const nlohmann::json &jObj);

private:
  shared_ptr<Expression> expr_;
  shared_ptr<Expression> fromLit_;
  shared_ptr<Expression> forLit_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Substr& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("expr", expr.expr_),
                                 f.field("fromLit", expr.fromLit_),
                                 f.field("forLit", expr.forLit_));
  }
};

shared_ptr<Expression> substr(const shared_ptr<Expression> &expr,
                              const shared_ptr<Expression> &fromLit,
                              const shared_ptr<Expression> &forLit);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_SUBSTR_H
