//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NUMERICLITERAL_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NUMERICLITERAL_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>

#include "Expression.h"

namespace normal::expression::gandiva {

template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
class NumericLiteral : public Expression {

public:
  explicit NumericLiteral(C_TYPE value) : value_(value) {}

  void compile(std::shared_ptr<arrow::Schema>) override {
    auto literal = ::gandiva::TreeExprBuilder::MakeLiteral(value_);

    gandivaExpression_ = literal;
    returnType_ = ::arrow::TypeTraits<ARROW_TYPE>::type_singleton();
  }

  std::string alias() override {
    if (typeid(ARROW_TYPE) == typeid(arrow::Int32Type) || typeid(ARROW_TYPE) == typeid(arrow::Int64Type)) {
      return prefixInt_ + std::to_string(value_);
    } else if (typeid(ARROW_TYPE) == typeid(arrow::FloatType)) {
      return prefixFloat_ + std::to_string(value_);
    }

    throw std::runtime_error("Numeric literal type not implemented");
  }

  std::shared_ptr<std::vector<std::string>> involvedColumnNames() override{
    return std::make_shared<std::vector<std::string>>();
  }

  C_TYPE value() const {
    return value_;
  }

private:
  C_TYPE value_;

};

template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
std::shared_ptr<Expression> num_lit(C_TYPE value){
  return std::make_shared<NumericLiteral<ARROW_TYPE>>(value);
}

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_NUMERICLITERAL_H
