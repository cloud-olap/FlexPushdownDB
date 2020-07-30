//
// Created by matt on 29/7/20.
//

#include "ArrayAppender.h"

#include <utility>

#include <fmt/format.h>

#include "StringArrayAppender.h"
#include "Int64ArrayAppender.h"

ArrayAppender::ArrayAppender(std::shared_ptr<::arrow::DataType> type,
							 std::shared_ptr<::arrow::ArrayBuilder> builder,
							 size_t expectedSize) :
	type_(std::move(type)),
	builder_(std::move(builder)),
	expectedSize_(expectedSize) {}

tl::expected<std::shared_ptr<ArrayAppender>, std::string>
ArrayAppender::make(const std::shared_ptr<::arrow::DataType> &type, size_t expectedSize = 0) {

  if (type->id() == ::arrow::StringType::type_id) {
	return std::make_shared<StringArrayAppender>(expectedSize);
  } else if (type->id() == ::arrow::Int64Type::type_id) {
	return std::make_shared<Int64ArrayAppender>(expectedSize);
  } else {
	return tl::make_unexpected(
		fmt::format("Appender for type '{}' not implemented yet", type->id()));
  }
}
