//
// Created by matt on 20/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_MERGE_MERGEKERNEL_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_MERGE_MERGEKERNEL_H

#include <vector>
#include <memory>
#include <string>

#include <tl/expected.hpp>

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::pushdown::merge {

class MergeKernel {

public:

  static tl::expected<void, std::string>
  validateTupleSets(const std::shared_ptr<TupleSet2> &tupleSet1, const std::shared_ptr<TupleSet2> &tupleSet2);

  static std::shared_ptr<Schema>
  mergeSchema(const std::shared_ptr<TupleSet2> &tupleSet1, const std::shared_ptr<TupleSet2> &tupleSet2);

  static std::vector<std::shared_ptr<::arrow::ChunkedArray>>
  mergeArrays(const std::shared_ptr<TupleSet2> &tupleSet1, const std::shared_ptr<TupleSet2> &tupleSet2);

  static tl::expected<std::shared_ptr<TupleSet2>, std::string>
  merge(const std::shared_ptr<TupleSet2> &tupleSet1, const std::shared_ptr<TupleSet2> &tupleSet2);
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_MERGE_MERGEKERNEL_H
