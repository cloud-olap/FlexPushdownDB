//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LITERAL_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LITERAL_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>

#include "Expression.h"

namespace normal::expression::gandiva {

template <typename T>
class Literal : public Expression {

public:
  explicit Literal(T value) : value_(value) {}

  void compile(std::shared_ptr<arrow::Schema>) override {
	auto literal = ::gandiva::TreeExprBuilder::MakeLiteral(value_);
	gandivaExpression_ = literal;
  }

  std::string alias() override {
	return std::to_string(value_);
  }

private:
  T value_;

};

template <typename T>
std::shared_ptr<Expression> lit(T value){
  return std::make_shared<Literal<T>>(value);
}

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LITERAL_H
