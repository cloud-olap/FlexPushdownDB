//
// Created by matt on 30/7/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_INT64ARRAYHASHER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_INT64ARRAYHASHER_H

#include "ArrayHasher.h"

namespace normal::tuple {

/**
 * Hasher for string arrays.
 */
class Int64ArrayHasher : public ArrayHasher {

public:
  explicit Int64ArrayHasher(const std::shared_ptr<::arrow::Array> &array);

  size_t hash(int64_t i) override;

private:
  std::shared_ptr<::arrow::Int64Array> int64Array_;
  std::hash<long> hash_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_INT64ARRAYHASHER_H
