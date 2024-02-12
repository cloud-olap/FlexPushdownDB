//
// Created by Yifei Yang on 12/10/21.
//

#include <fpdb/expression/gandiva/In.h>
#include <gandiva/tree_expr_builder.h>
#include <fmt/format.h>
#include <sstream>

namespace fpdb::expression::gandiva {

// makeGandivaExpression()
template<>
void In<arrow::Int32Type, int32_t>::makeGandivaExpression() {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionInt32(expr_->getGandivaExpression(), values_);
}

template<>
void In<arrow::Int64Type, int64_t>::makeGandivaExpression() {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionInt64(expr_->getGandivaExpression(), values_);
}

template<>
void In<arrow::DoubleType, double>::makeGandivaExpression() {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionDouble(expr_->getGandivaExpression(), values_);
}

template<>
void In<arrow::StringType, string>::makeGandivaExpression() {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionString(expr_->getGandivaExpression(), values_);
}

template<>
void In<arrow::Date64Type, int64_t>::makeGandivaExpression() {
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeInExpressionDate64(expr_->getGandivaExpression(), values_);
}


// alias()
template<>
string In<arrow::Int32Type, int32_t>::alias() {
  const auto &exprAlias = fmt::format("cast({} as int)",expr_->alias());
  stringstream ss;
  ss << "(";
  uint i = 0;
  for (const auto &value: values_) {
    ss << value;
    if (i < values_.size() - 1) {
      ss << ", ";
    }
    ++i;
  }
  ss << ")";
  return exprAlias + " in " + ss.str();
}

template<>
string In<arrow::Int64Type, int64_t>::alias() {
  const auto &exprAlias = fmt::format("cast({} as int)",expr_->alias());
  stringstream ss;
  ss << "(";
  uint i = 0;
  for (const auto &value: values_) {
    ss << value;
    if (i < values_.size() - 1) {
      ss << ", ";
    }
    ++i;
  }
  ss << ")";
  return exprAlias + " in " + ss.str();
}

template<>
string In<arrow::DoubleType, double>::alias() {
  const auto &exprAlias = fmt::format("cast({} as float)",expr_->alias());
  stringstream ss;
  ss << "(";
  uint i = 0;
  for (const auto &value: values_) {
    ss << value;
    if (i < values_.size() - 1) {
      ss << ", ";
    }
    ++i;
  }
  ss << ")";
  return exprAlias + " in " + ss.str();
}

template<>
string In<arrow::StringType, string>::alias() {
  stringstream ss;
  ss << "(";
  uint i = 0;
  for (const auto &value: values_) {
    ss << "\'" + value + "\'";
    if (i < values_.size() - 1) {
      ss << ", ";
    }
    ++i;
  }
  ss << ")";
  return expr_->alias() + " in " + ss.str();
}

template<>
string In<arrow::Date64Type, int64_t>::alias() {
  return "?column?";
}


// getTypeString()
template<>
string In<arrow::Int32Type, int32_t>::getTypeString() const {
  return "In<Int32>";
}

template<>
string In<arrow::Int64Type, int64_t>::getTypeString() const {
  return "In<Int64>";
}

template<>
string In<arrow::DoubleType, double>::getTypeString() const {
  return "In<Double>";
}

template<>
string In<arrow::StringType, string>::getTypeString() const {
  return "In<String>";
}

template<>
string In<arrow::Date64Type, int64_t>::getTypeString() const {
  return "In<Date64>";
}

}
