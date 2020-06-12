//
// Created by matt on 11/6/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_AND_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_AND_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"

namespace normal::expression::gandiva {

class And : public Expression {

public:
  And(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

private:
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;

};

std::shared_ptr<Expression> and_(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_AND_H