//
// Created by matt on 28/4/20.
//

#include <normal/expression/gandiva/NumericLiteral.h>
#include <gandiva/tree_expr_builder.h>

namespace normal::expression::gandiva {

template<>
void NumericLiteral<arrow::Int32Type>::compile(const std::shared_ptr<arrow::Schema> ) {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeLiteral(value_);
  returnType_ = arrow::int32();
}

template<>
void NumericLiteral<arrow::Int64Type>::compile(const std::shared_ptr<arrow::Schema> ) {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeLiteral(value_);
  returnType_ = arrow::int64();
}

template<>
void NumericLiteral<arrow::FloatType>::compile(const std::shared_ptr<arrow::Schema> ) {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeLiteral(value_);
  returnType_ = arrow::float32();
}

template<>
void NumericLiteral<arrow::DoubleType>::compile(const std::shared_ptr<arrow::Schema> ) {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeLiteral(value_);
  returnType_ = arrow::float64();
}

template<>
void NumericLiteral<arrow::BooleanType>::compile(const std::shared_ptr<arrow::Schema> ) {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeLiteral(value_);
  returnType_ = arrow::boolean();
}

}
