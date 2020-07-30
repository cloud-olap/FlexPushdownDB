//
// Created by matt on 30/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_ARRAYHASHER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_ARRAYHASHER_H

#include <memory>
#include <utility>

#include <arrow/api.h>
#include <tl/expected.hpp>
#include <fmt/format.h>

/**
 * Class for obtaining hashes from arrays. Subclassed for each tupe of array.
 */
class ArrayHasher {

public:
  explicit ArrayHasher(std::shared_ptr<::arrow::Array> array);
  virtual ~ArrayHasher() = default;

  static tl::expected<std::shared_ptr<ArrayHasher>, std::string>
  make(const std::shared_ptr<::arrow::Array> &array);

  virtual size_t hash(int64_t i) = 0;

protected:
  std::shared_ptr<::arrow::Array> array_;

};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_ARRAYHASHER_H
