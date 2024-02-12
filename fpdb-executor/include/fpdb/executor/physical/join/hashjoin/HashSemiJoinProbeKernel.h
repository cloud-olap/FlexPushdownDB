//
// Created by Yifei Yang on 12/15/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHSEMIJOINPROBEKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHSEMIJOINPROBEKERNEL_H

#include <fpdb/executor/physical/join/hashjoin/HashJoinProbeAbstractKernel.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/expression/gandiva/Filter.h>
#include <unordered_set>

namespace fpdb::executor::physical::join {

class HashSemiJoinProbeKernel: public HashJoinProbeAbstractKernel {

public:
  HashSemiJoinProbeKernel(HashJoinPredicate pred, set<string> neededColumnNames);
  HashSemiJoinProbeKernel() = default;
  HashSemiJoinProbeKernel(const HashSemiJoinProbeKernel&) = default;
  HashSemiJoinProbeKernel& operator=(const HashSemiJoinProbeKernel&) = default;
  
  static shared_ptr<HashSemiJoinProbeKernel> make(HashJoinPredicate pred, set<string> neededColumnNames);

  bool isSemi() const override;
  tl::expected<void, string> joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex>& tupleSetIndex) override;
  tl::expected<void, string> joinProbeTupleSet(const shared_ptr<TupleSet>& tupleSet) override;
  tl::expected<void, string> finalize() override;

  void clear() override;

private:
  static tl::expected<shared_ptr<::gandiva::SelectionVector>, string>
  makeSelectionVector(const unordered_set<int64_t> &rowMatchIndexes);

  unordered_set<int64_t> rowMatchIndexes_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashSemiJoinProbeKernel& kernel) {
    return f.object(kernel).fields(f.field("pred", kernel.pred_),
                                   f.field("neededColumnNames", kernel.neededColumnNames_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHSEMIJOINPROBEKERNEL_H
