//
// Created by Yifei Yang on 8/6/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_DOUBLEARRAYAPPENDER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_DOUBLEARRAYAPPENDER_H

#include "ArrayAppender.h"

namespace normal::tuple {

/**
 * Builder for double arrays.
 */
class DoubleArrayAppender : public ArrayAppender {
public:
  explicit DoubleArrayAppender(size_t expectedSize = 0);

  void appendValue(const std::shared_ptr<::arrow::Array> &array, int64_t i) override;

  tl::expected<std::shared_ptr<arrow::Array>, std::string> finalize() override;

private:
  std::vector<::arrow::DoubleType::c_type> buffer_;
  std::shared_ptr<::arrow::DoubleBuilder> doubleBuilder_;
};

}


#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_DOUBLEARRAYAPPENDER_H
