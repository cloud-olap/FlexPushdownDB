//
// Created by Yifei Yang on 12/10/21.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_IF_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_IF_H

#include "Expression.h"
#include <memory>

using namespace std;

namespace fpdb::expression::gandiva {

class If : public Expression {
public:
  If(const shared_ptr<Expression> &ifExpr,
     const shared_ptr<Expression> &thenExpr,
     const shared_ptr<Expression> &elseExpr);
  If() = default;
  If(const If&) = default;
  If& operator=(const If&) = default;

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() const override;
  set<string> involvedColumnNames() override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<If>, std::string> fromJson(const nlohmann::json &jObj);

private:
  shared_ptr<Expression> ifExpr_;
  shared_ptr<Expression> thenExpr_;
  shared_ptr<Expression> elseExpr_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, If& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("ifExpr", expr.ifExpr_),
                                 f.field("thenExpr", expr.thenExpr_),
                                 f.field("elseExpr", expr.elseExpr_));
  }
};

shared_ptr<Expression> if_(const shared_ptr<Expression> &ifExpr,
                           const shared_ptr<Expression> &thenExpr,
                           const shared_ptr<Expression> &elseExpr);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_IF_H
