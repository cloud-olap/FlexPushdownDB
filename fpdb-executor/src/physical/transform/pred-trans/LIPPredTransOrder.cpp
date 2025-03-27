//
// Created by Yifei Yang on 12/27/24.
//

#include <fpdb/executor/physical/transform/pred-trans/LIPPredTransOrder.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterSplitPOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterConcatPOp.h>
#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterInitPOp.h>
#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterFinalizePOp.h>
#include <fpdb/executor/physical/bloomfilter/DistGlobalBloomFilterInitPOp.h>
#include <fpdb/executor/physical/bloomfilter/DistGlobalBloomFilterMergePOp.h>
#include <fpdb/executor/physical/split/SplitPOp.h>
#include <fpdb/executor/physical/broadcast/BroadcastPOp.h>
#include <fpdb/executor/physical/project/TupleSetSizeProjectPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/plan/prephysical/Util.h>

namespace fpdb::executor::physical {

LIPPredTransOrder::LIPPredTransOrder(PrePToPTransformerForPredTrans* transformer):
  PredTransOrder(PredTransOrderType::LIP, transformer) {}

void LIPPredTransOrder::orderPredTrans(const JoinOriginSet &joinOrigins) {
  // Make star schema subtrees for LIP
  auto joinOriginMap = generateJoinOriginMap(joinOrigins);
  expandLIP(joinOriginMap, transformer_->getPrePhysicalPlan());

  // Make and connect Bloom filters in LIP-style
  makeAndConnectFilterOps();

  // update transformation results
  updateTransRes();

#if SHOW_DEBUG_METRICS == true
  // collect predicate transfer metrics
  for (const auto &ptUnitIt: ptUnits_) {
    uint prePOpId = ptUnitIt.second->base_->prePOpId_;
    if (dstPtUnits_.find(prePOpId) == dstPtUnits_.end()) {
      continue;
    }
    for (const auto &opVec: ptUnitIt.second->base_->currUpConn_) {
      for (const auto &op: opVec) {
        op->setCollPredTransMetrics({true, prePOpId,
                                     plan::prephysical::Util::getBaseTableDigest(ptUnitIt.second->prePOp_),
                                     metrics::PredTransMetrics::PTMetricsUnitType::PRED_TRANS});
      }
    }
  }
#endif
}

LIPPredTransOrder::JoinOriginKey
LIPPredTransOrder::makeJoinOriginKey(uint prePOpId1, uint prePOpId2) {
  return (prePOpId1 < prePOpId2) ?
      std::make_pair(prePOpId1, prePOpId2) : std::make_pair(prePOpId2, prePOpId1);
}

LIPPredTransOrder::JoinOriginMap LIPPredTransOrder::generateJoinOriginMap(
    const JoinOriginSet &joinOrigins) {
  JoinOriginMap joinOriginMap;
  for (const auto &joinOrigin: joinOrigins) {
    int leftPrePOpId = joinOrigin->left_->getId();
    int rightPrePOpId = joinOrigin->right_->getId();
    joinOriginMap[makeJoinOriginKey(leftPrePOpId, rightPrePOpId)] = joinOrigin;
  }
  return joinOriginMap;
}

void LIPPredTransOrder::expandLIP(const JoinOriginMap &joinOriginMap,
                                  const std::shared_ptr<PrePhysicalPlan> &prePhysicalPlan) {
  auto subtree = expandLIP_DFS(joinOriginMap, prePhysicalPlan->getRootOp());
  // check root level expand result
  if (subtree != nullptr && !subtree->dimensions_.empty()) {
    lipSubtrees_.emplace_back(subtree);
  }
}

std::shared_ptr<LIPPredTransOrder::StarSubtree> LIPPredTransOrder::expandLIP_DFS(
    const JoinOriginMap &joinOriginMap, const std::shared_ptr<PrePhysicalOp> &op) {
  switch (op->getType()) {
    case PrePOpType::FILTERABLE_SCAN: {
      // A leaf node, create an initail `StarSubtree` with `op` as the fact op
      return std::make_shared<StarSubtree>(op);
    }
    case PrePOpType::FILTER:
    case PrePOpType::PROJECT:
    case PrePOpType::GROUP:
    case PrePOpType::SORT:
    case PrePOpType::LIMIT_SORT: {
      // LIP filters can still be pushed down across these unary op types
      return expandLIP_DFS(joinOriginMap, op->getProducers()[0]);
    }
    case PrePOpType::HASH_JOIN: {
      // Try to expand the `StarSubtree` first, since in our join tree the hash table
      // is built on the left, actually we are checking if the subtree is right-deep
      auto leftSubtree = expandLIP_DFS(joinOriginMap, op->getProducers()[0]);
      auto rightSubtree = expandLIP_DFS(joinOriginMap, op->getProducers()[1]);
      if (leftSubtree != nullptr && rightSubtree != nullptr &&
          leftSubtree->dimensions_.empty()) {
        const auto &factOp = rightSubtree->factOp_;
        const auto &candidateDimensionOp = leftSubtree->factOp_;
        auto joinOriginIt = joinOriginMap.find(
          makeJoinOriginKey(factOp->getId(), candidateDimensionOp->getId()));
        if (joinOriginIt != joinOriginMap.end()) {
          const auto &joinOrigin = joinOriginIt->second;
          bool isJoinTypeQualified;
          auto joinType = joinOrigin->joinType_;
          std::vector<std::string> factColumns, dimensionColumns;
          if (factOp->getId() == joinOrigin->left_->getId()) {
            factColumns = joinOrigin->leftColumns_;
            dimensionColumns = joinOrigin->rightColumns_;
            isJoinTypeQualified = joinType != JoinType::FULL && joinType != JoinType::LEFT;
          } else {
            factColumns = joinOrigin->rightColumns_;
            dimensionColumns = joinOrigin->leftColumns_;
            isJoinTypeQualified = joinType != JoinType::FULL && joinType != JoinType::RIGHT;
          }
          if (isJoinTypeQualified) {
            join::HashJoinPredicate hashJoinPredicate(
              joinOrigin->leftColumns_, joinOrigin->rightColumns_);
            rightSubtree->dimensions_.emplace_back(candidateDimensionOp,
              factColumns, dimensionColumns, hashJoinPredicate.toString());
            return rightSubtree;
          }
        }
      }
      // If we cannot expand, then directly add the `StarSubtree`s from the producers
      if (leftSubtree != nullptr && !leftSubtree->dimensions_.empty()) {
        lipSubtrees_.emplace_back(leftSubtree);
      }
      if (rightSubtree != nullptr && !rightSubtree->dimensions_.empty()) {
        lipSubtrees_.emplace_back(rightSubtree);
      }
      return nullptr;
    }
    default: {
      // For the rest op types, LIP filters cannot be pushed down and we finalize the
      // so far expanded `StarSubtree`s here
      for (const auto &producer: op->getProducers()) {
        auto subtree = expandLIP_DFS(joinOriginMap, producer);
        if (subtree != nullptr && !subtree->dimensions_.empty()) {
          lipSubtrees_.emplace_back(subtree);
        }
      }
      return nullptr;
    }
  }
}

void LIPPredTransOrder::makeAndConnectFilterOps() {
  for (const auto &lipSubtree: lipSubtrees_) {
    makeAndConnectOneSubtreeFilterOps(lipSubtree);
  }
}

void LIPPredTransOrder::makeAndConnectOneSubtreeFilterOps(
        const std::shared_ptr<StarSubtree> &lipSubtree) {
  // Make or find `ptUnit` for `factOp_`
  auto factPTUnit = makePredTransUnit(lipSubtree->factOp_);
  dstPtUnits_.emplace(lipSubtree->factOp_->getId());
  for (const auto &dimension: lipSubtree->dimensions_) {
    // Make or find `ptUnit` for `dimensionOp_`
    auto dimensionPTUnit = makePredTransUnit(dimension.dimensionOp_);
    // Make and connect Bloom filters from `dimensionOp_` to `factOp_`
    uint lipFilterId = ptOpIdGen_.fetch_add(1);
    makeAndConnectOnePairFilterOps(lipFilterId, dimensionPTUnit, factPTUnit,
                                   dimension.hashJoinPredicateStr_,
                                   dimension.dimensionColumns_, dimension.factColumns_);
  }
}

void LIPPredTransOrder::
makeAndConnectOnePairFilterOps(uint lipFilterId,
                               const std::shared_ptr<PredTransUnit> &srcPTUnit,
                               const std::shared_ptr<PredTransUnit> &dstPTUnit,
                               const std::string &hashJoinPredicateStr,
                               const std::vector<std::string> &buildColumns,
                               const std::vector<std::string> &probeColumns) {
  // We adopt `BCAST-BF` design in `SmallToLargePredTransOrder.cpp` with `USE_DIST_GLOBAL_BF`
  // and `USE_PARALLEL_DIST_GLOBAL_BF_MERGE` on
  POpVec newOps;
  POpVec bfInitOps, bfFinalizeOps, bfBroadcastOps;
  POpVec in, out;
  std::vector<POpVec> filterProbe;
  for (int i = 0; i < transformer_->numNodes_; ++i) {
    // BF ops
    POpVec singleBfCreateVec, singleBfUseVec;
    for (int j = 0; j < transformer_->parallelDegree_; ++j) {
      int opId = j * transformer_->numNodes_ + i;
      singleBfCreateVec.emplace_back(std::make_shared<bloomfilter::BloomFilterCreatePOp>(
        fmt::format("BloomFilterCreate(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, opId),
        std::vector<std::string>{} /*unused*/,
        i,
        buildColumns,
        true));
      singleBfUseVec.emplace_back(std::make_shared<bloomfilter::BloomFilterUsePOp>(
        fmt::format("BloomFilterUse(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, opId),
        std::vector<std::string>{} /*unused*/,
        i,
        probeColumns,
        1));
    }
    newOps.insert(newOps.end(), singleBfCreateVec.begin(), singleBfCreateVec.end());
    newOps.insert(newOps.end(), singleBfUseVec.begin(), singleBfUseVec.end());
    // "GlobalBloomFilterInitPOp" for the build side and "SplitPOp" for the probe side
    std::shared_ptr<PhysicalOp> init = std::make_shared<bloomfilter::GlobalBloomFilterInitPOp>(
      fmt::format("GlobalBloomFilterInit(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i,
      buildColumns,
      1);
    bfInitOps.emplace_back(init);
    PrePToPTransformerUtil::connectOneToMany(init, singleBfCreateVec);
    std::shared_ptr<PhysicalOp> split = std::make_shared<split::SplitPOp>(
      fmt::format("Split(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i);
    PrePToPTransformerUtil::connectOneToMany(split, singleBfUseVec);
    newOps.insert(newOps.end(), {init, split});
    // set "out" and "filterProbe"
    out.emplace_back(split);
    filterProbe.emplace_back(singleBfUseVec);
    // "GlobalBloomFilterFinalizePOp"
    std::shared_ptr<PhysicalOp> finalize = std::make_shared<bloomfilter::GlobalBloomFilterFinalizePOp>(
      fmt::format("GlobalBloomFilterFinalize(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i);
    bloomfilter::GlobalBloomFilterFinalizePOp::connectToProducers(finalize, init, singleBfCreateVec);
    bfFinalizeOps.emplace_back(finalize);
    newOps.emplace_back(finalize);
    // broadcast bf across nodes
    std::shared_ptr<PhysicalOp> bfBroadcastOp = std::make_shared<broadcast::BroadcastPOp>(
      fmt::format("Broadcast-BF(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i);
    bfBroadcastOps.emplace_back(bfBroadcastOp);
    newOps.emplace_back(bfBroadcastOp);
    // connect "BroadcastPOp" to "BloomFilterUsePOp" to forward received remote bf
    PrePToPTransformerUtil::connectOneToMany(bfBroadcastOp, singleBfUseVec);
  }

  // "DistGlobalBloomFilterInit" after collecting input size from all nodes
  int nodeSyncDistBf = rand() % transformer_->numNodes_;
  std::shared_ptr<PhysicalOp> distBfInit = std::make_shared<DistGlobalBloomFilterInitPOp>(
    fmt::format("DistGlobalBloomFilterInitPOp(LIP)<{}>-{}", lipFilterId, hashJoinPredicateStr),
    std::vector<std::string>{} /*unused*/,
    nodeSyncDistBf);
  newOps.emplace_back(distBfInit);
  for (int i = 0; i < transformer_->numNodes_; ++i) {
    // collect build side input size from all nodes
    std::shared_ptr<PhysicalOp> buildInputBroadcast = std::make_shared<broadcast::BroadcastPOp>(
      fmt::format("Broadcast-BI(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i);
    std::shared_ptr<PhysicalOp> sizeProject = std::make_shared<project::TupleSetSizeProjectPOp>(
      fmt::format("TupleSetSizeProject(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i);
    newOps.insert(newOps.end(), {buildInputBroadcast, sizeProject});
    PrePToPTransformerUtil::connectOneToOne(buildInputBroadcast, sizeProject);
    PrePToPTransformerUtil::connectOneToOne(buildInputBroadcast, bfInitOps[i]);
    PrePToPTransformerUtil::connectOneToOne(sizeProject, distBfInit);
    PrePToPTransformerUtil::connectOneToOne(distBfInit, bfInitOps[i]);
    // set "in"
    in.emplace_back(buildInputBroadcast);
  }

  // create a separate bf split, bf concate and dist bf merge for each node
  POpVec bfSplitOps, bfConcatOps, distBfMergeOps;
  for (int i = 0; i < transformer_->numNodes_; ++i) {
    bfSplitOps.emplace_back(std::make_shared<BloomFilterSplitPOp>(
      fmt::format("BloomFilterSplitPOp(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i));
    bfConcatOps.emplace_back(std::make_shared<BloomFilterConcatPOp>(
      fmt::format("BloomFilterConcatPOp(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i));
    distBfMergeOps.emplace_back(std::make_shared<DistGlobalBloomFilterMergePOp>(
      fmt::format("DistGlobalBloomFilterMergePOp(LIP)<{}>-{}-{}", lipFilterId, hashJoinPredicateStr, i),
      std::vector<std::string>{} /*unused*/,
      i));
  }
  newOps.insert(newOps.end(), bfSplitOps.begin(), bfSplitOps.end());
  newOps.insert(newOps.end(), bfConcatOps.begin(), bfConcatOps.end());
  newOps.insert(newOps.end(), distBfMergeOps.begin(), distBfMergeOps.end());
  // connect bf finalize to bf split
  for (int i = 0; i < transformer_->numNodes_; ++i) {
    bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(bfFinalizeOps[i], {bfSplitOps[i]}, {});
  }
  // connect bf split to dist bf merge
  PrePToPTransformerUtil::connectManyToMany(bfSplitOps, distBfMergeOps);
  // connect dist bf merge to bf concat, need to do one by one for ordering needed by bf concat
  for (int i = 0; i < transformer_->numNodes_; ++i) {
    for (int j = 0; j < transformer_->numNodes_; ++j) {
      if (i == j) {
        bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(distBfMergeOps[i], {bfConcatOps[j]}, {});
      } else {
        bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(distBfMergeOps[i], {}, {bfConcatOps[j]});
      }
    }
  }
  // connect bf concat to bf broadcast
  PrePToPTransformerUtil::connectOneToOne(bfConcatOps, bfBroadcastOps);

  // add ops
  PrePToPTransformerUtil::addPhysicalOps(newOps, transformer_->physicalOps_);
  // connect bf ops to input ops, for src (build) side
  PrePToPTransformerUtil::connectManyToOneByGroup(srcPTUnit->base_->currUpConn_, in);
  // connect bf ops to input ops, for dst (probe) side
  PrePToPTransformerUtil::connectManyToOneByGroup(dstPTUnit->base_->currUpConn_, out);
  dstPTUnit->base_->currUpConn_ = filterProbe;

#if SHOW_DEBUG_METRICS == true
  // classify created ops into pred-trans phase
  for (const auto &op: newOps) {
    op->setPTPhaseType(metrics::PredTransMetrics::PRED_TRANS_PHASE);
  }
#endif
}

std::shared_ptr<LIPPredTransOrder::PredTransUnit>
LIPPredTransOrder::makePredTransUnit(const std::shared_ptr<PrePhysicalOp> &prePOp) {
  uint prePOpId = prePOp->getId();
  auto ptUnitIt = ptUnits_.find(prePOpId);
  if (ptUnitIt == ptUnits_.end()) {
    // transform the input table ops
    auto upConnPOpVec = transformer_->transformDfs(prePOp);
    auto initUpConn = PrePToPTransformerUtil::groupByNodeId(upConnPOpVec, transformer_->numNodes_);
    std::vector<POpVec> updatedUpConn;
    bool update = PrePToPTransformerUtil::spreadTableToAllNodes(prePOpId, transformer_->physicalOps_,
                                                                initUpConn, updatedUpConn);
    auto ptUnit = std::make_shared<PredTransUnit>(prePOp, update ? updatedUpConn : initUpConn);
    ptUnits_[prePOpId] = ptUnit;
    origUpConnToPTUnit_[prePOpId] = ptUnit->base_;
    return ptUnit;
  } else {
    return ptUnitIt->second;
  }
}

}
