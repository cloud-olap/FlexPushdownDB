//
// Created by Yifei Yang on 2/5/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_SPLITKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_SPLITKERNEL_H

#include <fpdb/tuple/TupleSet.h>
#include <tl/expected.hpp>

namespace fpdb::executor::physical::split {

class SplitKernel {
public:
  // old version which uses "CombineChunks()" before splitting
  static tl::expected<std::vector<std::shared_ptr<tuple::TupleSet>>, std::string>
  split(const std::shared_ptr<tuple::TupleSet> &tupleSet, uint n);

  // new version which does not "CombineChunks()" before splitting
  static tl::expected<std::vector<std::shared_ptr<tuple::TupleSet>>, std::string>
  split2(const std::shared_ptr<tuple::TupleSet> &tupleSet, uint n);

  // used when the byte size of `tupleSet` is larger than int32 max
  static tl::expected<std::vector<std::shared_ptr<tuple::TupleSet>>, std::string>
  splitForOverFlow(const std::shared_ptr<tuple::TupleSet> &tupleSet);
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_SPLITKERNEL_H
