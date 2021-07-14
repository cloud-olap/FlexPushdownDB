//
// Created by matt on 30/7/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYHASHER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYHASHER_H

#include <memory>
#include <utility>

#include <arrow/api.h>
#include <tl/expected.hpp>
#include <fmt/format.h>

namespace normal::tuple {

/**
 * Class for obtaining hashes from arrays. Subclassed for each type of array.
 */
class ArrayHasher {

public:
  virtual ~ArrayHasher() = default;

  static tl::expected<std::shared_ptr<ArrayHasher>, std::string>
  make(const std::shared_ptr<::arrow::Array> &array);

  virtual size_t hash(int64_t i) = 0;

};

template<typename CType, typename ArrowType>
class ArrayHasherWrapper : public ArrayHasher {

  using ArrowArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

public:
  explicit ArrayHasherWrapper(std::shared_ptr<ArrowArrayType> array) : array_(array) {}

  size_t hash(int64_t i) override {
    // overrided by each type
    return 0;
  }

private:
  std::shared_ptr<ArrowArrayType> array_;
  std::hash<CType> hash_;
  std::hash<::arrow::util::string_view> stringHash_;
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYHASHER_H
