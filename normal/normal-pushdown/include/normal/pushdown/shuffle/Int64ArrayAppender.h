//
// Created by matt on 30/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_INT64ARRAYAPPENDER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_INT64ARRAYAPPENDER_H

#include <cstdint>
#include "ArrayAppender.h"

namespace normal::pushdown::shuffle {

/**
 * Builder for int64 arrays.
 */
class Int64ArrayAppender : public ArrayAppender {
public:
  explicit Int64ArrayAppender(size_t expectedSize = 0);

  void appendValue(const std::shared_ptr<::arrow::Array> &array, int64_t i) override;

  tl::expected<std::shared_ptr<arrow::Array>, std::string> finalize() override;

private:
  std::vector<long> buffer_;
  std::shared_ptr<::arrow::Int64Builder> int64Builder_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_INT64ARRAYAPPENDER_H
