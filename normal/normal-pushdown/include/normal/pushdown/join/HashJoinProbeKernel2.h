//
// Created by matt on 31/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL2_H


#include <memory>

#include "JoinPredicate.h"
#include "HashTable.h"
#include "ArraySetIndex.h"

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::pushdown::join {

class HashJoinProbeKernel2 {

public:
  explicit HashJoinProbeKernel2(JoinPredicate pred);
  static HashJoinProbeKernel2 make(JoinPredicate pred);

  void putArraySetIndex(const std::shared_ptr<ArraySetIndex>& arraySetIndex);
  tl::expected<void, std::string> putTupleSet(const std::shared_ptr<TupleSet2>& tupleSet);

  tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> join();

private:
  JoinPredicate pred_;
  std::optional<std::shared_ptr<ArraySetIndex>> arraySetIndex_;
  std::optional<std::shared_ptr<TupleSet2>> tupleSet_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL2_H
