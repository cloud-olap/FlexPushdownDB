//
// Created by Yifei Yang on 8/6/20.
//

#include "normal/tuple/DoubleArrayAppender.h"

using namespace normal::tuple;

DoubleArrayAppender::DoubleArrayAppender(size_t expectedSize) :
        ArrayAppender(std::make_shared<arrow::DoubleType>(), std::make_shared<::arrow::DoubleBuilder>(), expectedSize) {
  buffer_.reserve(expectedSize_);
  doubleBuilder_ = std::static_pointer_cast<::arrow::DoubleBuilder>(builder_);
}

void DoubleArrayAppender::appendValue(const std::shared_ptr<::arrow::Array> &array, int64_t i) {
  buffer_.emplace_back(std::static_pointer_cast<::arrow::DoubleArray>(array)->Value(i));
}

tl::expected<std::shared_ptr<arrow::Array>, std::string> DoubleArrayAppender::finalize() {

  ::arrow::Status status;
  std::shared_ptr<::arrow::DoubleArray> array;

  buffer_.shrink_to_fit();

  status = doubleBuilder_->AppendValues(buffer_);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  status = doubleBuilder_->Finish(&array);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  buffer_.clear();

  return array;
}
