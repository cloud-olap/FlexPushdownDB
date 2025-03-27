//
// Created by Yifei Yang on 4/11/23.
//

#include <fpdb/executor/physical/transform/pred-trans/PrePToPTransformerForPredTrans.h>
#include <fpdb/executor/physical/transform/pred-trans/PredTransOrder.h>
#include <fpdb/executor/physical/transform/PrePToFPDBStorePTransformer.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/plan/prephysical/Util.h>
#include <fpdb/catalogue/obj-store/ObjStoreCatalogueEntry.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>
#include <queue>

namespace fpdb::executor::physical {

PrePToPTransformerForPredTrans::PrePToPTransformerForPredTrans(
        const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
        const shared_ptr<CatalogueEntry> &catalogueEntry,
        const shared_ptr<ObjStoreConnector> &objStoreConnector,
        const shared_ptr<Mode> &mode,
        int parallelDegree,
        int numNodes,
        // used by adapt exec
        long queryId,
        bool isDistributed,
        void* executor):
  PrePToPTransformer(prePhysicalPlan, catalogueEntry, objStoreConnector, mode,
                     parallelDegree, numNodes, queryId, isDistributed, executor) {
  type_ = PrePToPTransformerType::PRED_TRANS;
  if (mode->id() == ModeId::PUSHDOWN_ONLY || mode->id() == ModeId::HYBRID) {
    throw std::runtime_error("Predicate transfer with pushdown is not supported");
  }
}

std::shared_ptr<PhysicalPlan> PrePToPTransformerForPredTrans::transform() {
  // Phase 1
  transformPredTrans();

  // Phase 2
  auto upConnOps = transformExec();

  // make the final plan
  std::shared_ptr<PhysicalOp> collateOp = std::make_shared<collate::CollatePOp>(
          "Collate",
          ColumnName::canonicalize(prePhysicalPlan_->getOutputColumnNames()),
          0);
  PrePToPTransformerUtil::connectManyToOne(upConnOps, collateOp);
  PrePToPTransformerUtil::addPhysicalOps({collateOp}, physicalOps_);
  return std::make_shared<PhysicalPlan>(physicalOps_, collateOp->name());
}

void PrePToPTransformerForPredTrans::transformPredTrans() {
  // extract base table joins
  auto joinOrigins = JoinOriginTracer::trace(prePhysicalPlan_);

  // predicate transfer ordering
  PredTransOrder::orderPredTrans(PRED_TRANS_ORDER_TYPE, this, joinOrigins);
}

std::vector<std::shared_ptr<PhysicalOp>>
PrePToPTransformerForPredTrans::transformDfs(const std::shared_ptr<PrePhysicalOp> &prePOp) {
  // check if this prepOp has already been visited, since one may be visited multiple times
  auto transformResIt = prePOpToTransRes_.find(prePOp->getId());
  if (transformResIt != prePOpToTransRes_.end()) {
    return transformResIt->second;
  }

  // transform calling the base-class impl
  auto transRes = PrePToPTransformer::transformDfs(prePOp);

  // save transformation results
  prePOpToTransRes_[prePOp->getId()] = transRes;
  return transRes;
}

std::vector<std::shared_ptr<PhysicalOp>> PrePToPTransformerForPredTrans::transformExec() {
  // transform from root in dfs
  return transformDfs(prePhysicalPlan_->getRootOp());
}

}
