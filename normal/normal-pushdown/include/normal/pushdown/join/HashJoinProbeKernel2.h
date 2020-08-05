//
// Created by matt on 31/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL2_H


#include <memory>

#include "JoinPredicate.h"
#include "normal/pushdown/join/ATTIC/HashTable.h"
#include "normal/tuple/TupleSetIndex.h"

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::pushdown::join {

class HashJoinProbeKernel2 {

public:
  explicit HashJoinProbeKernel2(JoinPredicate pred);
  static HashJoinProbeKernel2 make(JoinPredicate pred);

  [[nodiscard]] tl::expected<void, std::string> putBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex>& tupleSetIndex);
  [[nodiscard]] tl::expected<void, std::string> putProbeTupleSet(const std::shared_ptr<TupleSet2>& tupleSet);

  tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> join();

private:
  JoinPredicate pred_;
  std::optional<std::shared_ptr<TupleSetIndex>> buildTupleSetIndex_;
  std::optional<std::shared_ptr<TupleSet2>> probeTupleSet_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL2_H
