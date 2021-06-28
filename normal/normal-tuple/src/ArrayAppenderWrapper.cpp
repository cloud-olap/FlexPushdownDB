//
// Created by matt on 13/8/20.
//

#include "normal/tuple/ArrayAppenderWrapper.h"

using namespace normal::tuple;

template<>
void
ArrayAppenderWrapper<std::string,
					 ::arrow::StringType>::appendValue(const std::shared_ptr<::arrow::Array> &array,
													   size_t i) {

  assert(array);
  assert(array->length() >= 0);
  assert(i < (size_t)array->length());

  buffer_.emplace_back(std::static_pointer_cast<ArrowArrayType>(array)->GetString(i));
}

template<>
tl::expected<void, std::string>
ArrayAppenderWrapper<std::string,
					 ::arrow::StringType>::safeAppendValue(const std::shared_ptr<::arrow::Array> &array,
														   size_t i) {
  if (!array)
	return tl::make_unexpected(
		fmt::format("Cannot append value. Null source array"));
  if (array->length() < 0)
	return tl::make_unexpected(
		fmt::format("Cannot append value. Source array with negative length {}", array->length()));
  if (i >= (size_t)array->length())
	return tl::make_unexpected(
		fmt::format("Cannot append value. Source index {} outside bounds of source array {}", i, array->length()));

  appendValue(array, i);

  return {};
}

template<>
::arrow::Status
ArrayAppenderWrapper<::arrow::Int32Type::c_type,
					 ::arrow::Int32Type>::appendValues(const std::shared_ptr<::arrow::Int32Builder> &builder,
														 const std::vector<::arrow::Int32Type::c_type> &buffer) {
  return builder->AppendValues(buffer);
}

template<>
::arrow::Status
ArrayAppenderWrapper<::arrow::Int64Type::c_type,
					 ::arrow::Int64Type>::appendValues(const std::shared_ptr<::arrow::Int64Builder> &builder,
														 const std::vector<::arrow::Int64Type::c_type> &buffer) {
  return builder->AppendValues(buffer);
}

template<>
::arrow::Status
ArrayAppenderWrapper<::arrow::DoubleType::c_type,
					 ::arrow::DoubleType>::appendValues(const std::shared_ptr<::arrow::DoubleBuilder> &builder,
														  const std::vector<::arrow::DoubleType::c_type> &buffer) {
  return builder->AppendValues(buffer);
}

template<>
::arrow::Status
ArrayAppenderWrapper<std::string,
					 ::arrow::StringType>::appendValues(const std::shared_ptr<::arrow::StringBuilder> &builder,
														  const std::vector<std::string> &buffer) {
  return builder->AppendValues(buffer);
}