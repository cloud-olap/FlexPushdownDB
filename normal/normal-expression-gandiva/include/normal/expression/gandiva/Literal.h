//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LITERAL_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LITERAL_H

#include <memory>
#include <arrow/api.h>
#include "Expression.h"

namespace normal::expression::gandiva {

template <typename T>
class Literal : public Expression {
private:
  T value_;
public:
  std::shared_ptr<::gandiva::Node> buildGandivaExpression(std::shared_ptr<arrow::Schema> Ptr) override {
	return std::shared_ptr<::gandiva::Node>();
  }
  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema> Ptr) override {
	return std::shared_ptr<arrow::DataType>();
  }
  void compile(std::shared_ptr<arrow::Schema> schema) override {

  }
  std::string name() override {
	return "literal";
  }
public:
  explicit Literal(T value) : value_(value) {}

};

template <typename T>
static std::shared_ptr<Expression> lit(T value){
  return std::make_shared<Literal<T>>(value);
}

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LITERAL_H
