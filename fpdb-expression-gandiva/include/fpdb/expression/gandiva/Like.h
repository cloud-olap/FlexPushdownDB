//
// Created by Yifei Yang on 12/11/21.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_LIKE_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_LIKE_H

#include "BinaryExpression.h"
#include <string>
#include <memory>

namespace fpdb::expression::gandiva {

class Like : public BinaryExpression {

public:
  Like(const shared_ptr<Expression>& left, const shared_ptr<Expression>& right);
  Like() = default;
  Like(const Like&) = default;
  Like& operator=(const Like&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  static tl::expected<std::shared_ptr<Like>, std::string> fromJson(const nlohmann::json &jObj);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Like& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("left", expr.left_),
                                 f.field("right", expr.right_));
  }
};

shared_ptr<Expression> like(const shared_ptr<Expression>& left, const shared_ptr<Expression>& right);

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_LIKE_H
