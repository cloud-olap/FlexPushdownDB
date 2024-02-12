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
  UNKNOWN
};

class PredTransOrder {

public:
  static void orderPredTrans(
          PredTransOrderType type,
          PrePToPTransformerForPredTrans* transformer,
          const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins);

  PredTransOrder(PredTransOrderType type,
                 PrePToPTransformerForPredTrans* transformer);
  virtual ~PredTransOrder() = default;

  PredTransOrderType getType() const;

private:
  /**
   * Make the order of predicate transfer
   * Updated parameters: physicalOps, prePOpToTransRes
   */
  virtual void orderPredTrans(const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash,
          JoinOriginPtrPred> &joinOrigins) = 0;

  PredTransOrderType type_;

protected:
  // basic unit for predicate transfer, i.e. ops (scan/local filter, BF create/use) corresponding to a single scan op
  // a unit can be viewed as a vertical chain from scan/local filter to subsequent BF use ops.
  struct PredTransUnitBase {
    uint prePOpId_;       // the prephysical op id of the corresponding FilterableScanPrePOp
    std::shared_ptr<PhysicalOp> origUpConnOp_;    // the start of the vertical chain, also as the identifier
    std::shared_ptr<PhysicalOp> currUpConnOp_;    // the end of the vertical chain

    PredTransUnitBase(uint prePOpId, const std::shared_ptr<PhysicalOp> &upConnOp):
      prePOpId_(prePOpId), origUpConnOp_(upConnOp), currUpConnOp_(upConnOp) {}

    size_t hash() const {
      return std::hash<std::string>()(origUpConnOp_->name());
    }

    bool equalTo(const std::shared_ptr<PredTransUnitBase> &other) const {
      return origUpConnOp_->name() == other->origUpConnOp_->name();
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
  std::atomic<uint> ptOpIdGen_ = 0;

  // keep track of the ops that generate the input tables (predicate-transfer filtered) for Phase 2 plan
  std::unordered_map<std::string, std::shared_ptr<PredTransUnitBase>> origUpConnOpToPTUnit_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_PREDTRANSORDER_H
