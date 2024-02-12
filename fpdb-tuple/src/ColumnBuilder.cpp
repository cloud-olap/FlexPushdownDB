//
// Created by matt on 8/5/20.
//

#include "fpdb/tuple/ColumnBuilder.h"

using namespace fpdb::tuple;

ColumnBuilder::ColumnBuilder(std::string name, const std::shared_ptr<::arrow::DataType> &type) :name_(std::move(name)) {
  auto arrowStatus = ::arrow::MakeBuilder(::arrow::default_memory_pool(), type, &arrowBuilder_);
}

std::shared_ptr<ColumnBuilder> ColumnBuilder::make(const std::string &name,
												   const std::shared_ptr<::arrow::DataType> &type) {
  return std::make_unique<ColumnBuilder>(name, type);
}

tl::expected<void, std::string> ColumnBuilder::append(const std::shared_ptr<Scalar> &scalar) {
  ::arrow::Status status;
  auto rawBuilderPtr = arrowBuilder_.get();

  if (scalar->type()->id() == ::arrow::Int32Type::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::Int32Builder*>(rawBuilderPtr);
    auto expValue = scalar->value<int>();
    if (!expValue) {
      return tl::make_unexpected(expValue.error());
    }
    status = typedArrowBuilder->Append(*expValue);
  }
  else if (scalar->type()->id() == ::arrow::Int64Type::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::Int64Builder*>(rawBuilderPtr);
    auto expValue = scalar->value<long>();
    if (!expValue) {
      return tl::make_unexpected(expValue.error());
    }
    status = typedArrowBuilder->Append(*expValue);
  }
  else if (scalar->type()->id() == ::arrow::DoubleType::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::DoubleBuilder*>(rawBuilderPtr);
    auto expValue = scalar->value<double>();
    if (!expValue) {
      return tl::make_unexpected(expValue.error());
    }
    status = typedArrowBuilder->Append(*expValue);
  }
  else if (scalar->type()->id() == ::arrow::BooleanType::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::BooleanBuilder*>(rawBuilderPtr);
    auto expValue = scalar->value<bool>();
    if (!expValue) {
      return tl::make_unexpected(expValue.error());
    }
    status = typedArrowBuilder->Append(*expValue);
  }
  else if (scalar->type()->id() == ::arrow::Date64Type::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::Date64Builder*>(rawBuilderPtr);
    auto expValue = scalar->value<long>();
    if (!expValue) {
      return tl::make_unexpected(expValue.error());
    }
    status = typedArrowBuilder->Append(*expValue);
  }
  else if (scalar->type()->id() == ::arrow::StringType::type_id) {
    auto typedArrowBuilder = dynamic_cast<::arrow::StringBuilder*>(rawBuilderPtr);
    auto expValue = scalar->value<std::string>();
    if (!expValue) {
      return tl::make_unexpected(expValue.error());
    }
    status = typedArrowBuilder->Append(*expValue);
  }
  else {
    return tl::make_unexpected("Builder for type '" + scalar->type()->ToString() + "' not implemented yet");
  }

  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  return {};
}

tl::expected<void, std::string> ColumnBuilder::appendNulls(int64_t length) {
  auto status = arrowBuilder_->AppendNulls(length);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  return {};
}

tl::expected<std::shared_ptr<Column>, std::string> ColumnBuilder::finalize() {
  auto status = arrowBuilder_->Finish(&array_);
  if (!status.ok()) {
    return tl::make_unexpected("Error when finalizing ColumnBuilder, " + status.message());
  }
  auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(array_);
  return std::make_shared<Column>(name_, chunkedArray);
}

tl::expected<std::shared_ptr<arrow::Array>, std::string> ColumnBuilder::finalizeToArray() {
  auto status = arrowBuilder_->Finish(&array_);
  if (!status.ok()) {
    return tl::make_unexpected("Error when finalizing ColumnBuilder, " + status.message());
  }
  return array_;
}
