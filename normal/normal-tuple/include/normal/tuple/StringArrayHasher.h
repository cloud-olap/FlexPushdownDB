//
// Created by matt on 30/7/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_STRINGARRAYHASHER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_STRINGARRAYHASHER_H

#include "ArrayHasher.h"

namespace normal::tuple {

/**
 * Hasher for string arrays.
 */
class StringArrayHasher : public ArrayHasher {

public:
  explicit StringArrayHasher(const std::shared_ptr<::arrow::Array> &array);

  size_t hash(int64_t i) override;

private:
  std::shared_ptr<::arrow::StringArray> stringArray_;
  std::hash<::arrow::util::string_view> hash_;
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_STRINGARRAYHASHER_H
