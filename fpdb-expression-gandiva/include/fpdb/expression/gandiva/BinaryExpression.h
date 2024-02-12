//
// Created by Yifei Yang on 7/17/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_BINARYEXPRESSION_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_BINARYEXPRESSION_H

#include <string>
#include <memory>

#include "Expression.h"

using namespace std;

namespace fpdb::expression::gandiva {

class BinaryExpression : public Expression {

public:
  BinaryExpression(shared_ptr<Expression> left,
                   shared_ptr<Expression> right,
                   ExpressionType type);
  BinaryExpression() = default;
  BinaryExpression(const BinaryExpression&) = default;
  BinaryExpression& operator=(const BinaryExpression&) = default;

  /**
   * Deal with int32 vs int64, int64 vs double, int32 vs double
   */
  std::tuple<shared_ptr<arrow::DataType>, ::gandiva::NodePtr, ::gandiva::NodePtr> castGandivaExprToUpperType();
  set<string> involvedColumnNames() override;
  virtual ::nlohmann::json toJson() const override;
  static tl::expected<std::pair<std::shared_ptr<Expression>, std::shared_ptr<Expression>>, std::string>
  fromJson(const nlohmann::json &jObj);

  [[nodiscard]] const shared_ptr<Expression> &getLeft() const;
  [[nodiscard]] const shared_ptr<Expression> &getRight() const;

  [[maybe_unused]] void setLeft(const shared_ptr<Expression> &left);
  [[maybe_unused]] void setRight(const shared_ptr<Expression> &right);

protected:
  shared_ptr<Expression> left_;
  shared_ptr<Expression> right_;

  string genAliasForComparison(const string& compOp);
};

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_BINARYEXPRESSION_H
