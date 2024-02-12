//
// Created by Yifei Yang on 12/10/21.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_IN_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_IN_H

#include "Expression.h"
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <unordered_set>
#include <fmt/format.h>

using namespace std;

namespace fpdb::expression::gandiva {

template<typename ARROW_TYPE, typename C_TYPE>
class In : public Expression {

public:
  In(const shared_ptr<Expression> &expr, const unordered_set<C_TYPE> &values):
    Expression(IN),
    expr_(expr),
    values_(values) {}
  In() = default;
  In(const In&) = default;
  In& operator=(const In&) = default;

  void compile(const shared_ptr<arrow::Schema> &schema) override {
    expr_->compile(schema);
    returnType_ = arrow::boolean();
    makeGandivaExpression();
  }

  void makeGandivaExpression();

  string alias() override;

  string getTypeString() const override;

  set<string> involvedColumnNames() override {
    return expr_->involvedColumnNames();
  }

  ::nlohmann::json toJson() const override {
    ::nlohmann::json jObj;
    jObj.emplace("type", "In");
    jObj.emplace("dataType", fpdb::tuple::ArrowSerializer::dataType_to_bytes(
            ::arrow::TypeTraits<ARROW_TYPE>::type_singleton()));
    jObj.emplace("expr", expr_->toJson());
    jObj.emplace("values", values_);
    return jObj;
  }

  static tl::expected<std::shared_ptr<In>, std::string> fromJson(const nlohmann::json &jObj) {
    if (!jObj.contains("expr")) {
      return tl::make_unexpected(fmt::format("Expr not specified in In expression JSON '{}'", to_string(jObj)));
    }
    auto expExpr = Expression::fromJson(jObj["expr"]);
    if (!expExpr) {
      return tl::make_unexpected(expExpr.error());
    }

    if (!jObj.contains("values")) {
      return tl::make_unexpected(fmt::format("Values not specified in In expression JSON '{}'", to_string(jObj)));
    }
    auto values = jObj["values"].get<std::unordered_set<C_TYPE>>();

    return std::make_shared<In>(*expExpr, values);
  }

private:
  shared_ptr<Expression> expr_;
  unordered_set<C_TYPE> values_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, In& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("expr", expr.expr_),
                                 f.field("values", expr.values_));
  }
};

template<typename ARROW_TYPE, typename C_TYPE>
shared_ptr<Expression> in(const shared_ptr<Expression> &expr, const unordered_set<C_TYPE> &values) {
  return make_shared<In<ARROW_TYPE, C_TYPE>>(expr, values);
}

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_IN_H
