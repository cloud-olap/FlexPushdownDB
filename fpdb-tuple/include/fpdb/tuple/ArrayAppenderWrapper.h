//
// Created by matt on 13/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARRAYAPPENDERWRAPPER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARRAYAPPENDERWRAPPER_H

#include <utility>

#include <tl/expected.hpp>

#include "fpdb/tuple/ArrayAppender.h"
#include "fpdb/tuple/Globals.h"

namespace fpdb::tuple {

template<typename CType, typename ArrowType>
class ArrayAppenderWrapper : public ArrayAppender {

  using ArrowArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;
  using ArrowBuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;

public:
  explicit ArrayAppenderWrapper(size_t expectedSize = 0) :
	  ArrayAppender() {
    buffer_.reserve(expectedSize);
    builder_ = std::make_shared<ArrowBuilderType>();
  }

  static std::shared_ptr<ArrayAppenderWrapper> make(size_t expectedSize = 0) {
	  return std::make_shared<ArrayAppenderWrapper>(expectedSize);
  }

  static ::arrow::Status appendValues(const std::shared_ptr<ArrowBuilderType> &builder,
                                      const std::vector<CType> &buffer,
                                      const std::vector<bool> &isValid);

  tl::expected<void, std::string> appendValue(const std::shared_ptr<::arrow::Array> &array, int64_t i) override {
    // check array and index
    if (!array)
      return tl::make_unexpected(
        fmt::format("Cannot append value. Null source array"));
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
      buffer_.emplace_back(std::static_pointer_cast<ArrowArrayType>(array)->Value(i));
      isValid_.emplace_back(true);
      return {};
    }
  }

  tl::expected<std::shared_ptr<arrow::Array>, std::string> finalize() override {
    ::arrow::Status status;
    std::shared_ptr<ArrowArrayType> array;

    buffer_.shrink_to_fit();

    status = appendValues(builder_, buffer_, isValid_);
    if (!status.ok()) {
      return tl::make_unexpected(status.message());
    }

    status = builder_->Finish(&array);
    if (!status.ok()) {
      return tl::make_unexpected(status.message());
    }

    buffer_.clear();
    return array;
  }

private:
  tl::expected<void, std::string> appendNull() override {
    buffer_.emplace_back(NULL);
    isValid_.emplace_back(false);
    return {};
  }

  std::vector<bool> isValid_;
  std::vector<CType> buffer_;
  std::shared_ptr<ArrowBuilderType> builder_;

};

template<>
tl::expected<void, std::string> ArrayAppenderWrapper<std::string, ::arrow::StringType>::appendValue(
        const std::shared_ptr<::arrow::Array> &array, int64_t i);

template<>
tl::expected<void, std::string> ArrayAppenderWrapper<std::string, ::arrow::StringType>::appendNull();


class ArrayAppenderBuilder {

public:
  static tl::expected<std::shared_ptr<ArrayAppender>, std::string> make(const std::shared_ptr<::arrow::DataType> &type,
                                                                        size_t expectedSize = 0) {
    if (type->id() == ::arrow::StringType::type_id) {
      return ArrayAppenderWrapper<std::string, ::arrow::StringType>::make(expectedSize);
    } else if (type->id() == ::arrow::Int32Type::type_id) {
      return ArrayAppenderWrapper<::arrow::Int32Type::c_type, ::arrow::Int32Type>::make(expectedSize);
    } else if (type->id() == ::arrow::Int64Type::type_id) {
      return ArrayAppenderWrapper<::arrow::Int64Type::c_type, ::arrow::Int64Type>::make(expectedSize);
    } else if (type->id() == ::arrow::DoubleType::type_id) {
      return ArrayAppenderWrapper<::arrow::DoubleType::c_type, ::arrow::DoubleType>::make(expectedSize);
    } else if (type->id() == ::arrow::Date32Type::type_id) {
      return ArrayAppenderWrapper<::arrow::Date32Type::c_type, ::arrow::Date32Type>::make(expectedSize);
    } else if (type->id() == ::arrow::Date64Type::type_id) {
      return ArrayAppenderWrapper<::arrow::Date64Type::c_type, ::arrow::Date64Type>::make(expectedSize);
    } else if (type->id() == ::arrow::BooleanType::type_id) {
      return ArrayAppenderWrapper<::arrow::BooleanType::c_type, ::arrow::BooleanType>::make(expectedSize);
    } else {
	    return tl::make_unexpected(
	            fmt::format("ArrayAppender not implemented for type '{}'", type->name()));
	  }
  }
};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARRAYAPPENDERWRAPPER_H
