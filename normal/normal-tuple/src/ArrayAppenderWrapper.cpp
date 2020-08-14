//
// Created by matt on 13/8/20.
//

#include "normal/tuple/ArrayAppenderWrapper.h"

using namespace normal::tuple;

template<>
void ArrayAppenderWrapper<std::string, ::arrow::StringType>::appendValue(const std::shared_ptr<::arrow::Array> &array, int64_t i) {
	buffer_.emplace_back(std::static_pointer_cast<ArrowArrayType>(array)->GetString(i));
}

template<>
::arrow::Status ArrayAppenderWrapper<::arrow::Int32Type::c_type, ::arrow::Int32Type>::strangeProblem(const std::shared_ptr<::arrow::Int32Builder> &builder, const std::vector<::arrow::Int32Type::c_type> &buffer){
  return builder->AppendValues(buffer);
}

template<>
::arrow::Status ArrayAppenderWrapper<::arrow::Int64Type::c_type, ::arrow::Int64Type>::strangeProblem(const std::shared_ptr<::arrow::Int64Builder> &builder, const std::vector<::arrow::Int64Type::c_type> &buffer){
  return builder->AppendValues(buffer);
}

template<>
::arrow::Status ArrayAppenderWrapper<::arrow::DoubleType::c_type, ::arrow::DoubleType>::strangeProblem(const std::shared_ptr<::arrow::DoubleBuilder> &builder, const std::vector<::arrow::DoubleType::c_type> &buffer){
  return builder->AppendValues(buffer);
}

template<>
::arrow::Status ArrayAppenderWrapper<std::string, ::arrow::StringType>::strangeProblem(const std::shared_ptr<::arrow::StringBuilder> &builder, const std::vector<std::string> &buffer){
  return builder->AppendValues(buffer);
}