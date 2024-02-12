//
// Created by Yifei Yang on 4/11/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_PREPTOPTRANSFORMERFORPREDTRANS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_PREPTOPTRANSFORMERFORPREDTRANS_H

#include <fpdb/executor/physical/transform/PrePToPTransformer.h>
#include <fpdb/executor/metrics/PredTransMetrics.h>
#include <fpdb/plan/prephysical/JoinOriginTracer.h>

using namespace fpdb::plan::prephysical;

namespace fpdb::executor::physical {

/**
 * Used specifically for predicate transfer
 */
class PrePToPTransformerForPredTrans: public PrePToPTransformer {

public:
  static std::shared_ptr<PhysicalPlan> transform(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                                 const shared_ptr<CatalogueEntry> &catalogueEntry,
                                                 const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                                 const shared_ptr<Mode> &mode,
                                                 int parallelDegree,
                                                 int numNodes);

  friend class SmallToLargePredTransOrder;
  friend class BFSPredTransOrder;
  friend class PredTransOrder;

private:
  PrePToPTransformerForPredTrans(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                 const shared_ptr<CatalogueEntry> &catalogueEntry,
                                 const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                 const shared_ptr<Mode> &mode,
                                 int parallelDegree,
                                 int numNodes);

  /**
   * Main entry for transformation
   */
  std::shared_ptr<PhysicalPlan> transform() override;
  // basically just add a "transRes_" cache during the visit
  std::vector<std::shared_ptr<PhysicalOp>> transformDfs(const std::shared_ptr<PrePhysicalOp> &prePOp) override;

  /**
   * Phase 1: generate a partial plan for predicate transfer using bloom filters
   */
  void transformPredTrans();

  /**
   * Phase 2: generate a partial plan for execution after predicate transfer, and connect to the partial plan of Phase 1
   * @return physical ops to be connected to the final collate op
   */
  std::vector<std::shared_ptr<PhysicalOp>> transformExec();

  /**
   * states maintained during transformation
   */
  // saved transformation results
  std::unordered_map<int, std::vector<std::shared_ptr<PhysicalOp>>> prePOpToTransRes_;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_PREPTOPTRANSFORMERFORPREDTRANS_H
