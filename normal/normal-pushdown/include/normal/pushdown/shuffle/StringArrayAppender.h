//
// Created by matt on 29/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_STRINGARRAYAPPENDER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_STRINGARRAYAPPENDER_H

#include "ArrayAppender.h"

/**
 * Builder for string arrays.
 */
class StringArrayAppender : public ArrayAppender {
public:
  explicit StringArrayAppender(int64_t expectedSize = 0);

  void appendValue(const std::shared_ptr<::arrow::Array> &accessor, int64_t i) override;

  tl::expected<std::shared_ptr<arrow::Array>, std::string> finalize() override;

private:
  std::vector<std::string> buffer_;
  std::shared_ptr<::arrow::StringBuilder> stringBuilder_;

};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_STRINGARRAYAPPENDER_H
