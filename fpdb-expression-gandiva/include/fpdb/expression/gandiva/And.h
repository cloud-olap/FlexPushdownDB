//
// Created by matt on 11/6/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_AND_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_AND_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"

using namespace std;

namespace fpdb::expression::gandiva {

class And : public Expression {

public:
  And(const vector<shared_ptr<Expression>>& exprs);
  And() = default;
  And(const And&) = default;
  And& operator=(const And&) = default;

  void compile(const shared_ptr<arrow::Schema> &schema) override;
  string alias() override;
  string getTypeString() const override;
  set<string> involvedColumnNames() override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<And>, std::string> fromJson(const nlohmann::json &jObj);

  const vector<shared_ptr<Expression>> &getExprs() const;

private:
  vector<shared_ptr<Expression>> exprs_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, And& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("exprs", expr.exprs_));
  }
};

shared_ptr<Expression> and_(const shared_ptr<Expression>& left, const shared_ptr<Expression>& right);
shared_ptr<Expression> and_(const vector<shared_ptr<Expression>> &exprs);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_AND_H
