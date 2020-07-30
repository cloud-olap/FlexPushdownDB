//
// Created by matt on 30/7/20.
//

#include "normal/pushdown/shuffle/Int64ArrayAppender.h"

using namespace normal::pushdown::shuffle;

Int64ArrayAppender::Int64ArrayAppender(size_t expectedSize) :
	ArrayAppender(::arrow::int64(), std::make_shared<::arrow::Int64Builder>(), expectedSize) {
  buffer_.reserve(expectedSize_);
  int64Builder_ = std::static_pointer_cast<::arrow::Int64Builder>(builder_);
}

void Int64ArrayAppender::appendValue(const std::shared_ptr<::arrow::Array> &array, int64_t i) {
  buffer_.emplace_back(std::static_pointer_cast<::arrow::Int64Array>(array)->Value(i));
}

tl::expected<std::shared_ptr<arrow::Array>, std::string> Int64ArrayAppender::finalize() {

  ::arrow::Status status;
  std::shared_ptr<::arrow::Int64Array> array;

  buffer_.shrink_to_fit();

  status = int64Builder_->AppendValues(buffer_);
  if (!status.ok()) {
	return tl::make_unexpected(status.message());
  }

  status = int64Builder_->Finish(&array);
  if (!status.ok()) {
	return tl::make_unexpected(status.message());
  }

  buffer_.clear();

  return array;
}
