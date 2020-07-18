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
  BinaryExpression(const std::shared_ptr<Expression> &left, const std::shared_ptr<Expression> &right);

public:

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

  const std::shared_ptr<Expression> &getLeft() const;
  const std::shared_ptr<Expression> &getRight() const;

protected:
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;

};

}


#endif //NORMAL_BINARYEXPRESSION_H
