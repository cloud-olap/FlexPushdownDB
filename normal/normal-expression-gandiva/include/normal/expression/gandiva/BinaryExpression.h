//
// Created by Yifei Yang on 7/17/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_BINARYEXPRESSION_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_BINARYEXPRESSION_H

#include <string>
#include <memory>

#include "Expression.h"

namespace normal::expression::gandiva {

class BinaryExpression : public Expression {

public:
  BinaryExpression(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

  std::shared_ptr<std::vector<std::string> > involvedColumnNames() override;

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

  [[nodiscard]] const std::shared_ptr<Expression> &getLeft() const;
  [[nodiscard]] const std::shared_ptr<Expression> &getRight() const;

  [[maybe_unused]] void setLeft(const std::shared_ptr<Expression> &left);
  [[maybe_unused]] void setRight(const std::shared_ptr<Expression> &right);

protected:
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;

  std::string genAliasForComparison(const std::string& compOp);
};

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_BINARYEXPRESSION_H
