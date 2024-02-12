//
// Created by matt on 31/7/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL_H

#include <fpdb/executor/physical/join/hashjoin/HashJoinProbeAbstractKernel.h>
#include <fpdb/executor/physical/join/hashjoin/RecordBatchHashJoiner.h>
#include <fpdb/executor/physical/join/OuterJoinHelper.h>

namespace fpdb::executor::physical::join {

class HashJoinProbeKernel: public HashJoinProbeAbstractKernel {

public:
  HashJoinProbeKernel(HashJoinPredicate pred,
                      set<string> neededColumnNames,
                      bool isLeft,
                      bool isRight);
  HashJoinProbeKernel() = default;
  HashJoinProbeKernel(const HashJoinProbeKernel&) = default;
  HashJoinProbeKernel& operator=(const HashJoinProbeKernel&) = default;
  
  static shared_ptr<HashJoinProbeKernel> make(HashJoinPredicate pred,
                                              set<string> neededColumnNames,
                                              bool isLeft,
                                              bool isRight);

  bool isSemi() const override;
  tl::expected<void, string> joinBuildTupleSetIndex(const shared_ptr<TupleSetIndex>& tupleSetIndex) override;
  tl::expected<void, string> joinProbeTupleSet(const shared_ptr<TupleSet>& tupleSet) override;
  tl::expected<void, string> finalize() override;

  void clear() override;

private:
  tl::expected<shared_ptr<fpdb::tuple::TupleSet>, string> join(const shared_ptr<RecordBatchHashJoiner> &joiner,
                                                                 const shared_ptr<TupleSet> &probeTupleSet,
                                                                 int64_t probeRowOffset);
  tl::expected<void, string> makeOuterJoinHelpers();
  tl::expected<void, string> computeOuterJoin();

  bool isLeft_;
  bool isRight_;
  bool isOuterJoinHelperCreated_ = false;
  std::optional<shared_ptr<OuterJoinHelper>> leftJoinHelper_;
  std::optional<shared_ptr<OuterJoinHelper>> rightJoinHelper_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinProbeKernel& kernel) {
    return f.object(kernel).fields(f.field("pred", kernel.pred_),
                                   f.field("neededColumnNames", kernel.neededColumnNames_),
                                   f.field("isLeft", kernel.isLeft_),
                                   f.field("isRight", kernel.isRight_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINPROBEKERNEL_H
