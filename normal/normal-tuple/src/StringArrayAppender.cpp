//
// Created by matt on 29/7/20.
//

#include "normal/tuple/StringArrayAppender.h"

using namespace normal::tuple;

StringArrayAppender::StringArrayAppender(int64_t expectedSize) :
	ArrayAppender(::arrow::utf8(), std::make_shared<::arrow::StringBuilder>(), expectedSize) {
  buffer_.reserve(expectedSize_);
  stringBuilder_ = std::static_pointer_cast<::arrow::StringBuilder>(builder_);
}

void StringArrayAppender::appendValue(const std::shared_ptr<::arrow::Array> &array, int64_t i) {
  buffer_.emplace_back(std::static_pointer_cast<::arrow::StringArray>(array)->GetString(i));
}

tl::expected<std::shared_ptr<arrow::Array>, std::string> StringArrayAppender::finalize() {

  ::arrow::Status status;
  std::shared_ptr<::arrow::StringArray> array;

  buffer_.shrink_to_fit();

  status = stringBuilder_->AppendValues(buffer_);
  if (!status.ok()) {
	return tl::make_unexpected(status.message());
  }

  status = stringBuilder_->Finish(&array);
  if (!status.ok()) {
	return tl::make_unexpected(status.message());
  }

  buffer_.clear();

  return array;
}




