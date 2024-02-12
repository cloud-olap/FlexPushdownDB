//
// Created by matt on 28/4/20.
//

#include <fpdb/expression/gandiva/NumericLiteral.h>
#include <gandiva/tree_expr_builder.h>

namespace fpdb::expression::gandiva {

template<>
void NumericLiteral<arrow::Int32Type>::compile(const std::shared_ptr<arrow::Schema> &) {
  returnType_ = arrow::int32();
  gandivaExpression_ = value_.has_value() ?
          ::gandiva::TreeExprBuilder::MakeLiteral(*value_) :
          ::gandiva::TreeExprBuilder::MakeNull(returnType_);
}

template<>
void NumericLiteral<arrow::Int64Type>::compile(const std::shared_ptr<arrow::Schema> &) {
  returnType_ = arrow::int64();
  gandivaExpression_ = value_.has_value() ?
          ::gandiva::TreeExprBuilder::MakeLiteral(*value_) :
          ::gandiva::TreeExprBuilder::MakeNull(returnType_);
}

template<>
void NumericLiteral<arrow::DoubleType>::compile(const std::shared_ptr<arrow::Schema> &) {
  returnType_ = arrow::float64();
  gandivaExpression_ = value_.has_value() ?
          ::gandiva::TreeExprBuilder::MakeLiteral(*value_) :
          ::gandiva::TreeExprBuilder::MakeNull(returnType_);
}

template<>
void NumericLiteral<arrow::BooleanType>::compile(const std::shared_ptr<arrow::Schema> &) {
  returnType_ = arrow::boolean();
  gandivaExpression_ = value_.has_value() ?
          ::gandiva::TreeExprBuilder::MakeLiteral(*value_) :
          ::gandiva::TreeExprBuilder::MakeNull(returnType_);
}

template<>
void NumericLiteral<arrow::Date64Type>::compile(const std::shared_ptr<arrow::Schema> &) {
  returnType_ = arrow::date64();
  ::gandiva::NodePtr literal = value_.has_value() ?
          ::gandiva::TreeExprBuilder::MakeLiteral(*value_) :
          ::gandiva::TreeExprBuilder::MakeNull(returnType_);
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction("castDate", {literal}, returnType_);
}

template<>
std::string NumericLiteral<arrow::Int32Type>::getTypeString() const {
  return "NumericLiteral<Int32>";
}

template<>
std::string NumericLiteral<arrow::Int64Type>::getTypeString() const {
  return "NumericLiteral<Int64>";
}

template<>
std::string NumericLiteral<arrow::DoubleType>::getTypeString() const {
  return "NumericLiteral<Double>";
}

template<>
std::string NumericLiteral<arrow::BooleanType>::getTypeString() const {
  return "NumericLiteral<Boolean>";
}

template<>
std::string NumericLiteral<arrow::Date64Type>::getTypeString() const {
  return "NumericLiteral<Date64>";
}

}
