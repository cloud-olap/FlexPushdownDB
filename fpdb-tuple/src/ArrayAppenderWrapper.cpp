//
// Created by matt on 13/8/20.
//

#include "fpdb/tuple/ArrayAppenderWrapper.h"

using namespace fpdb::tuple;

template<>
tl::expected<void, std::string> ArrayAppenderWrapper<std::string, ::arrow::StringType>::appendValue(
        const std::shared_ptr<::arrow::Array> &array, int64_t i) {
  // check array and index
  if (!array)
	  return tl::make_unexpected(fmt::format("Cannot append value. Null source array"));
  if (array->length() < 0)
	  return tl::make_unexpected(
	          fmt::format("Cannot append value. Source array with negative length {}", array->length()));
  if (i >= array->length())
	  return tl::make_unexpected(
	          fmt::format("Cannot append value. Source index {} outside bounds of source array {}", i, array->length()));

  // check if the value at given index is null
  if (array->IsNull(i)) {
    return appendNull();
  } else {
    buffer_.emplace_back(std::static_pointer_cast<ArrowArrayType>(array)->GetString((int64_t) i));
    isValid_.emplace_back(true);
    return {};
  }
}

template<>
tl::expected<void, std::string> ArrayAppenderWrapper<std::string, ::arrow::StringType>::appendNull() {
  buffer_.emplace_back("");
  isValid_.emplace_back(false);
  return {};
}

template<>
::arrow::Status ArrayAppenderWrapper<::arrow::Int32Type::c_type, ::arrow::Int32Type>::appendValues(
        const std::shared_ptr<::arrow::Int32Builder> &builder,
        const std::vector<::arrow::Int32Type::c_type> &buffer,
        const std::vector<bool> &isValid) {
  return builder->AppendValues(buffer, isValid);
}

template<>
::arrow::Status ArrayAppenderWrapper<::arrow::Int64Type::c_type, ::arrow::Int64Type>::appendValues(
        const std::shared_ptr<::arrow::Int64Builder> &builder,
        const std::vector<::arrow::Int64Type::c_type> &buffer,
        const std::vector<bool> &isValid) {
  return builder->AppendValues(buffer, isValid);
}

template<>
::arrow::Status ArrayAppenderWrapper<::arrow::DoubleType::c_type, ::arrow::DoubleType>::appendValues(
        const std::shared_ptr<::arrow::DoubleBuilder> &builder,
        const std::vector<::arrow::DoubleType::c_type> &buffer,
        const std::vector<bool> &isValid) {
  return builder->AppendValues(buffer, isValid);
}

template<>
::arrow::Status ArrayAppenderWrapper<::arrow::Date32Type::c_type, ::arrow::Date32Type>::appendValues(
        const std::shared_ptr<::arrow::Date32Builder> &builder,
        const std::vector<::arrow::Date32Type::c_type> &buffer,
        const std::vector<bool> &isValid) {
  return builder->AppendValues(buffer, isValid);
}

template<>
::arrow::Status ArrayAppenderWrapper<::arrow::Date64Type::c_type, ::arrow::Date64Type>::appendValues(
        const std::shared_ptr<::arrow::Date64Builder> &builder,
        const std::vector<::arrow::Date64Type::c_type> &buffer,
        const std::vector<bool> &isValid) {
  return builder->AppendValues(buffer, isValid);
}

template<>
::arrow::Status ArrayAppenderWrapper<::arrow::BooleanType::c_type, ::arrow::BooleanType>::appendValues(
        const std::shared_ptr<::arrow::BooleanBuilder> &builder,
        const std::vector<::arrow::BooleanType::c_type> &buffer,
        const std::vector<bool> &isValid) {
  return builder->AppendValues(buffer, isValid);
}

template<>
::arrow::Status ArrayAppenderWrapper<std::string, ::arrow::StringType>::appendValues(
        const std::shared_ptr<::arrow::StringBuilder> &builder,
        const std::vector<std::string> &buffer,
        const std::vector<bool> &isValid) {
  // FIXME: is this a good way to convert vector<bool> to uint_8*?
  auto *validBytes = new uint8_t[isValid.size()];
  for (uint i = 0; i < isValid.size(); ++i) {
    if (isValid[i]) {
      validBytes[i] = 1;
    } else {
      validBytes[i] = 0;
    }
  }
  auto status = builder->AppendValues(buffer, validBytes);
  delete[](validBytes);
  return status;
}