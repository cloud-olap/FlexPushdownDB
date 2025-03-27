//
// Created by Yifei Yang on 5/15/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_PREDTRANSORDER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_PREDTRANSORDER_H

#include <fpdb/executor/physical/transform/pred-trans/PrePToPTransformerForPredTrans.h>
#include <fpdb/plan/prephysical/JoinOriginTracer.h>

using namespace fpdb::plan::prephysical;

namespace fpdb::executor::physical {

enum PredTransOrderType {
  SMALL_TO_LARGE,
  BFS,
  LIP,
  UNKNOWN
};

using POpVec = std::vector<std::shared_ptr<PhysicalOp>>;
using JoinOriginSet = std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred>;

class PredTransOrder {

public:
  static void orderPredTrans(
          PredTransOrderType type,
          PrePToPTransformerForPredTrans* transformer,
          const JoinOriginSet &joinOrigins);

  PredTransOrder(PredTransOrderType type,
                 PrePToPTransformerForPredTrans* transformer);
  virtual ~PredTransOrder() = default;

  PredTransOrderType getType() const;

private:
  /**
   * Make the order of predicate transfer
   * Updated parameters: physicalOps, prePOpToTransRes
   */
  virtual void orderPredTrans(const JoinOriginSet &joinOrigins) = 0;

  PredTransOrderType type_;

protected:
  // basic unit for predicate transfer, i.e. ops (scan/local filter, BF create/use) corresponding to a single scan op
  // a unit can be viewed as a vertical chain from scan/local filter to subsequent BF use ops.
  struct PredTransUnitBase {
    uint prePOpId_;       // identifier, the prephysical op id of the corresponding FilterableScanPrePOp
    std::vector<POpVec> origUpConn_;    // the start of the vertical chain, ops are per node
    std::vector<POpVec> currUpConn_;    // the end of the vertical chain, ops are per node

    PredTransUnitBase(uint prePOpId, const std::vector<POpVec> &upConn):
      prePOpId_(prePOpId), origUpConn_(upConn), currUpConn_(upConn) {}

    size_t hash() const {
      return prePOpId_;
    }

    bool equalTo(const std::shared_ptr<PredTransUnitBase> &other) const {
      return prePOpId_ == other->prePOpId_;
    }
  };

  // Update the ops that generate the input tables (predicate-transfer filtered) for Phase 2 plan.
  // I.e., the ops are originally scan/local filter, and may be expanded to BF use by Phase 1 plan.
  void updateTransRes();

  PrePToPTransformerForPredTrans* transformer_;     // the transformer that calls to order predicate transfer

  /**
   * states maintained during transformation
   */
  // generate unique id for ops create during pred-trans (bf / semi-join)
  // note this is different from prePOpId used for other ops
  // in SmallToLargePredTransOrder this is unique for each join origin (up to 4 ops share one)
  // in BFSPredTransOrder this is unique for each pair of bf create/use (2 ops share one), and also used for hash-join
  // ops for vanilla Yannakakis
  // in LIPPredTransOrder this is unique for each LIP filter
  std::atomic<uint> ptOpIdGen_ = 0;

  // map prePOpId to the ops that generate the input tables (predicate-transfer filtered) for Phase 2 plan
  std::unordered_map<uint, std::shared_ptr<PredTransUnitBase>> origUpConnToPTUnit_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_PREDTRANSORDER_H
