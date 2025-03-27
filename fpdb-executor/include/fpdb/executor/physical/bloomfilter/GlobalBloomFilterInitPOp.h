//
// Created by Yifei Yang on 10/24/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALBLOOMFILTERINITPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALBLOOMFILTERINITPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/tuple/TupleSet.h>

namespace fpdb::executor::physical::bloomfilter {

class GlobalBloomFilterInitPOp: public PhysicalOp {
public:
  GlobalBloomFilterInitPOp(const std::string &name,
                           const std::vector<std::string> &projectColumns,
                           int nodeId,
                           const std::vector<std::string> &bloomFilterColumns,
                           int numDistSubBf);
  GlobalBloomFilterInitPOp() = default;
  GlobalBloomFilterInitPOp(const GlobalBloomFilterInitPOp&) = default;
  GlobalBloomFilterInitPOp& operator=(const GlobalBloomFilterInitPOp&) = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void clear() override;

  void produceFinalize(const std::shared_ptr<PhysicalOp> &op);
  void recordPredTransCard(const executor::cache::PredTransCardCache::PredTransCardKey &key);

private:
  void onStart();
  void onTupleSet(const TupleSetMessage &message);
  void onComplete(const CompleteMessage &);

  std::vector<std::string> bloomFilterColumns_;
  int numDistSubBf_;
  std::string bfFinalizeOp_;

  std::vector<std::shared_ptr<TupleSet>> input_;
  int64_t numRows_ = 0;
  std::optional<DistGlobalArrowBFInitMessage> distInitMsg_ = std::nullopt;   // used if to build a single dist global BF

  // to record runtime cardinalities
  executor::cache::PredTransCardCache::PredTransCardInfo ptCardInfo_;
  std::optional<double> keyLen_ = std::nullopt;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, GlobalBloomFilterInitPOp& op) {
    return inspect_base(f, op,
                        f.field("bloomFilterColumns", op.bloomFilterColumns_),
                        f.field("numDistSubBf", op.numDistSubBf_),
                        f.field("bfFinalizeOp", op.bfFinalizeOp_),
                        f.field("ptCardInfo", op.ptCardInfo_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALBLOOMFILTERINITPOP_H
