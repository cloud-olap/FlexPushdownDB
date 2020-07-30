//
// Created by matt on 30/7/20.
//

#include "StringArrayHasher.h"

StringArrayHasher::StringArrayHasher(const std::shared_ptr<::arrow::Array> &array) :
	ArrayHasher(array) {
  stringArray_ = std::static_pointer_cast<::arrow::StringArray>(array_);
}

size_t StringArrayHasher::hash(int64_t i) {
  return hash_(stringArray_->GetView(i));
}

