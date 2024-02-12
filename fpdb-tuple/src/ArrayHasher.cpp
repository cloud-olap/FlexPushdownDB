//
// Created by matt on 30/7/20.
//

#include "fpdb/tuple/ArrayHasher.h"
#include "fpdb/util/Util.h"
#include <utility>

using namespace fpdb::tuple;
using namespace fpdb::util;

size_t ArrayHasher::hash(const std::vector<std::shared_ptr<ArrayHasher>> &hashers, int64_t row) {
  std::vector<size_t> hashes;
  hashes.reserve(hashers.size());
  for (const auto &hasher: hashers) {
    hashes.emplace_back(hasher->hash(row));
  }
  return hashCombine(hashes);
}

tl::expected<std::shared_ptr<ArrayHasher>, std::string>
ArrayHasher::make(const std::shared_ptr<::arrow::Array> &array) {

	if (array->type_id() == ::arrow::Int32Type::type_id) {
		auto typedArray = std::static_pointer_cast<::arrow::Int32Array>(array);
		return std::make_shared<ArrayHasherWrapper<::arrow::Int32Type::c_type, ::arrow::Int32Type>>(typedArray);
	} else if (array->type_id() == ::arrow::Int64Type::type_id) {
		auto typedArray = std::static_pointer_cast<::arrow::Int64Array>(array);
		return std::make_shared<ArrayHasherWrapper<::arrow::Int64Type::c_type, ::arrow::Int64Type>>(typedArray);
	} else if (array->type_id() == ::arrow::DoubleType::type_id) {
    auto typedArray = std::static_pointer_cast<::arrow::DoubleArray>(array);
    return std::make_shared<ArrayHasherWrapper<::arrow::DoubleType::c_type, ::arrow::DoubleType>>(typedArray);
  } else if (array->type_id() == ::arrow::Date64Type::type_id) {
    auto typedArray = std::static_pointer_cast<::arrow::Date64Array>(array);
    return std::make_shared<ArrayHasherWrapper<::arrow::Date64Type::c_type, ::arrow::Date64Type>>(typedArray);
  } else if (array->type_id() == ::arrow::StringType::type_id) {
		auto typedArray = std::static_pointer_cast<::arrow::StringArray>(array);
		return std::make_shared<ArrayHasherWrapper<std::string, ::arrow::StringType>>(typedArray);
  } else {
		return tl::make_unexpected(
		fmt::format("ArrayHasher for type '{}' not implemented yet", array->type()->name()));
  }
}

template<>
size_t ArrayHasherWrapper<::arrow::Int32Type::c_type, ::arrow::Int32Type>::hash(int64_t i) {
	return hash_(array_->GetView(i));
}

template<>
size_t ArrayHasherWrapper<::arrow::Int64Type::c_type, ::arrow::Int64Type>::hash(int64_t i) {
	return hash_(array_->GetView(i));
}

template<>
size_t ArrayHasherWrapper<::arrow::DoubleType::c_type, ::arrow::DoubleType>::hash(int64_t i) {
	return hash_(array_->GetView(i));
}

template<>
size_t ArrayHasherWrapper<std::string, ::arrow::StringType>::hash(int64_t i) {
	return stringHash_(array_->GetView(i));
}