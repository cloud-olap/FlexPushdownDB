//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_MULTIPLY_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_MULTIPLY_H

#include "Expression.h"
#include <arrow/api.h>
#include <memory>

namespace normal::expression::gandiva {

class Multiply : public Expression {
private:
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;

public:
  Multiply(std::shared_ptr<Expression> left,
		   std::shared_ptr<Expression> right)
	  : left_(std::move(left)), right_(std::move(right)) {}
  void compile(std::shared_ptr<arrow::Schema> schema) override {

  }
  std::string name() override {
	return std::__cxx11::string();
  }
  ::gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> Ptr) override {
	return ::gandiva::NodePtr();
  }

  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema> Ptr) override {
	return std::shared_ptr<arrow::DataType>();
  }
};

static std::shared_ptr<Expression>
times(std::shared_ptr<Expression> left,
	  std::shared_ptr<Expression> right) {
  return std::make_shared<Multiply>(std::move(left), std::move(right));
}

}
#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_MULTIPLY_H
