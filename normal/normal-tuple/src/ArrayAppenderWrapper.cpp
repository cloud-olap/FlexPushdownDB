//
// Created by matt on 13/8/20.
//

#include "normal/tuple/ArrayAppenderWrapper.h"

using namespace normal::tuple;

template<>
void ArrayAppenderWrapper<std::string, ::arrow::StringType>::appendValue(const std::shared_ptr<::arrow::Array> &array, int64_t i) {
	buffer_.emplace_back(std::static_pointer_cast<ArrowArrayType>(array)->GetString(i));
}