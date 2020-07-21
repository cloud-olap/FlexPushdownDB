//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_ADD_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_ADD_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"
#include "BinaryExpression.h"

namespace normal::expression::gandiva {

class Add : public BinaryExpression {

public:
  Add(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

};

std::shared_ptr<Expression> plus(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_ADD_H
