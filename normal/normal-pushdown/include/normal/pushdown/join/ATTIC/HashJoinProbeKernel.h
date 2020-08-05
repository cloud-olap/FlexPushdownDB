//
// Created by matt on 31/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL_H

#include <memory>

#include "normal/pushdown/join/JoinPredicate.h"
#include "HashTable.h"

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::pushdown::join {

class HashJoinProbeKernel {

public:
  explicit HashJoinProbeKernel(JoinPredicate pred);
  static HashJoinProbeKernel make(JoinPredicate pred);

  void putHashTable(const std::shared_ptr<HashTable>& hashTable);
  tl::expected<void, std::string> putTupleSet(const std::shared_ptr<TupleSet2>& tupleSet);

  tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> join();

private:
  JoinPredicate pred_;

  std::optional<std::shared_ptr<HashTable>> hashTable_;
  std::optional<std::shared_ptr<TupleSet2>> tupleSet_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBEKERNEL_H
