//
// Created by Yifei Yang on 4/27/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_JOIN_HASHJOIN_HASHJOINARROWPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_JOIN_HASHJOIN_HASHJOINARROWPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinArrowKernel.h>
#include <fpdb/executor/metrics/Globals.h>

namespace fpdb::executor::physical::join {

/**
 * Physical operator that does entire hash-join on top of arrow's exec kernel
 */
class HashJoinArrowPOp: public PhysicalOp {

public:
  HashJoinArrowPOp(const std::string &name,
                   const std::vector<std::string> &projectColumnNames,
                   int nodeId,
                   const HashJoinPredicate &pred,
                   JoinType joinType);
  HashJoinArrowPOp() = default;
  HashJoinArrowPOp(const HashJoinArrowPOp&) = default;
  HashJoinArrowPOp& operator=(const HashJoinArrowPOp&) = default;
  ~HashJoinArrowPOp() override = default;

  void onReceive(const Envelope &msg) override;
  void clear() override;
  void clearProducers() override;
  std::string getTypeString() const override;
  void setProjectColumnNames(const std::vector<std::string> &projectColumnNames) override;

  const HashJoinArrowKernel &getKernel() const;
  const std::set<std::string> &getBuildProducers() const;
  const std::set<std::string> &getProbeProducers() const;
  void setBuildProducers(const std::set<std::string> &buildProducers);
  void setProbeProducers(const std::set<std::string> &probeProducers);
  void addBuildProducer(const std::shared_ptr<PhysicalOp> &buildProducer);
  void addProbeProducer(const std::shared_ptr<PhysicalOp> &probeProducer);
  void clearBuildProducers();
  void clearProbeProducers();

  void recordOutputCard(const executor::cache::OutputCardCache::OutputCardKey &key, bool build);

#if SHOW_DEBUG_METRICS == true
  int64_t getNumRowsBuild() const;
  int64_t getNumRowsProbe() const;
#endif

private:
  void onStart();
  void onComplete(const CompleteMessage &message);
  void onTupleSet(const TupleSetMessage &message);
  void send();
  void sendEmpty();

  std::set<std::string> buildProducers_;
  std::set<std::string> probeProducers_;

  HashJoinArrowKernel kernel_;
  bool sentResult_ = false;
  int numCompletedBuildProducers_ = 0;
  int numCompletedProbeProducers_ = 0;

  // to record runtime cardinalities
  int64_t numRowsBuild_ = 0;
  int64_t numRowsProbe_ = 0;
  executor::cache::OutputCardCache::OutputCardInfo buildOutputCardInfo_, probeOutputCardInfo_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinArrowPOp& op) {
    return inspect_base(f, op,
                        f.field("buildProducers", op.buildProducers_),
                        f.field("probeProducers", op.probeProducers_),
                        f.field("kernel", op.kernel_),
                        f.field("buildOutputCardInfo", op.buildOutputCardInfo_),
                        f.field("probeOutputCardInfo", op.probeOutputCardInfo_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_JOIN_HASHJOIN_HASHJOINARROWPOP_H
