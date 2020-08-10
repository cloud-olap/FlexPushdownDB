//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LITERAL_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LITERAL_H

#include <string>
#include <memory>
#include <utility>

#include <arrow/api.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>

#include "Expression.h"

namespace normal::expression::gandiva {

template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
class Literal : public Expression {

public:
  explicit Literal(C_TYPE value) : value_(value) {}

  void compile(std::shared_ptr<arrow::Schema>) override {
	auto literal = ::gandiva::TreeExprBuilder::MakeLiteral(value_);

	gandivaExpression_ = literal;
	returnType_ = ::arrow::TypeTraits<ARROW_TYPE>::type_singleton();
  }

  std::string alias() override {
	return std::to_string(value_);
  }

private:
  C_TYPE value_;

};

template<>
class Literal<::arrow::StringType,std::string> : public Expression {

public:
  explicit Literal(std::string value) : value_(std::move(value)) {}

  void compile(std::shared_ptr<arrow::Schema>) override {
	auto literal = ::gandiva::TreeExprBuilder::MakeStringLiteral(value_);

	gandivaExpression_ = literal;
	returnType_ = ::arrow::TypeTraits<::arrow::StringType>::type_singleton();
  }

  std::string alias() override {
	return value_;
  }

private:
  std::string value_;

};

template<typename ARROW_TYPE, typename C_TYPE>
std::shared_ptr<Expression> lit(C_TYPE value){
  return std::make_shared<Literal<ARROW_TYPE, C_TYPE>>(value);
}

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_LITERAL_H
