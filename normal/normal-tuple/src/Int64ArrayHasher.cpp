//
// Created by matt on 30/7/20.
//

#include "normal/tuple/Int64ArrayHasher.h"

using namespace normal::tuple;

Int64ArrayHasher::Int64ArrayHasher(const std::shared_ptr<::arrow::Array> &array) :
	ArrayHasher(array) {
  int64Array_ = std::static_pointer_cast<::arrow::Int64Array>(array_);
}

size_t Int64ArrayHasher::hash(int64_t i) {
  return hash_(int64Array_->GetView(i));
}