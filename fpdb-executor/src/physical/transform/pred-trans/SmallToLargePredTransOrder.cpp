//
// Created by Yifei Yang on 5/15/23.
//

#include <fpdb/executor/physical/transform/pred-trans/SmallToLargePredTransOrder.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterInitPOp.h>
#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterFinalizePOp.h>
#include <fpdb/executor/physical/split/SplitPOp.h>
#include <fpdb/executor/physical/adaptive/AdaptSinkPOp.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/Executor.h>
#include <fpdb/plan/prephysical/Util.h>
#include <sstream>

namespace fpdb::executor::physical {

SmallToLargePredTransOrder::SmallToLargePredTransOrder(PrePToPTransformerForPredTrans* transformer):
  PredTransOrder(PredTransOrderType::SMALL_TO_LARGE, transformer) {}

void SmallToLargePredTransOrder::orderPredTrans(const JoinOriginSet &joinOrigins) {
  if (DIST_PRED_TRANS_TYPE == DistPredTransType::ADAPT) {
    if (transformer_->executor_ == nullptr) {
      throw std::runtime_error("Executor not set during adaptive pred-trans.");
    }
    if (USE_DOUBLE_EXEC_ADAPT) {
      // still generate a static query plan in once, but produced adaptively based on cardinalities of old run
      orderPredTransStatic(joinOrigins);
    } else {
      orderPredTransAdapt(joinOrigins);
    }
  } else {
    orderPredTransStatic(joinOrigins);
  }

  // show dist-PT types if needed
  showAdaptSelectedDistPTTypes();
}

void SmallToLargePredTransOrder::orderPredTransStatic(const JoinOriginSet &joinOrigins) {
  // create pred-trans filters (e.g., join key vals, bloom filters) both forward and backward
  makeFilterOps(joinOrigins);

  // connect forward filter ops
  connectFilterOps(true);

  // connect backward filter ops
  connectFilterOps(false);

  // update transformation results
  updateTransRes();

#if SHOW_DEBUG_METRICS == true
  // get a map from prePOpId to PrePhysicalOp
  std::unordered_map<uint, std::shared_ptr<PrePhysicalOp>> prePOpMap;
  for (const auto &joinOrigin: joinOrigins) {
    prePOpMap[joinOrigin->left_->getId()] = joinOrigin->left_;
    prePOpMap[joinOrigin->right_->getId()] = joinOrigin->right_;
  }
  // collect predicate transfer metrics
  for (const auto &ptUnitIt: ptUnits_) {
    uint prePOpId = ptUnitIt.second->base_->prePOpId_;
    for (const auto &opVec: ptUnitIt.second->base_->currUpConn_) {
      for (const auto &op: opVec) {
        op->setCollPredTransMetrics({true, prePOpId,
                                     plan::prephysical::Util::getBaseTableDigest(prePOpMap[prePOpId]),
                                     metrics::PredTransMetrics::PTMetricsUnitType::PRED_TRANS});
      }
    }
  }
#endif
}

void SmallToLargePredTransOrder::orderPredTransAdapt(const JoinOriginSet &joinOrigins) {
  // init adaptive exec
  ((Executor*)(transformer_->executor_))->initAdaptExec(transformer_->queryId_, transformer_->isDistributed_);

  // exec local filter stage
  execLocalFilterStage(joinOrigins);

  // exec each transfer step separately, adaptive exec is done during connecting PT units
  connectFilterOps(true, true);
  connectFilterOps(false, true);

  // update transformation results
  updateTransRes();
}

void SmallToLargePredTransOrder::makeFilterOps(const JoinOriginSet &joinOrigins) {
  for (const auto &joinOrigin: joinOrigins) {
    // transform the join origin ops
    auto upLeftConnPOpVec = transformer_->transformDfs(joinOrigin->left_);
    auto upRightConnPOpVec = transformer_->transformDfs(joinOrigin->right_);

    // cannot transfer on FULL joins
    if (joinOrigin->joinType_ == JoinType::FULL) {
      continue;
    }

    // make or find predicate transfer units
    auto leftPTUnit = makePredTransUnit(joinOrigin->left_, upLeftConnPOpVec, joinOrigin->leftColumns_);
    auto rightPTUnit = makePredTransUnit(joinOrigin->right_, upRightConnPOpVec, joinOrigin->rightColumns_);

    // make filter ops
    join::HashJoinPredicate hashJoinPredicate(joinOrigin->leftColumns_, joinOrigin->rightColumns_);
    const auto &hashJoinPredicateStr = hashJoinPredicate.toString();
    uint step = ptOpIdGen_.fetch_add(1);

    // forward pred-trans filter, blocked by right joins
    if (joinOrigin->joinType_ != JoinType::RIGHT) {
      makeOnePairFilterOps(true, joinOrigin, step, hashJoinPredicateStr, leftPTUnit, rightPTUnit);
    }

    // backward pred-trans filter, blocked by left joins
    if (joinOrigin->joinType_ != JoinType::LEFT) {
      makeOnePairFilterOps(false, joinOrigin, step, hashJoinPredicateStr, leftPTUnit, rightPTUnit);
    }
  }

  // find out different but identical ptUnits
  makePrePOpDisjointSet();
}

void SmallToLargePredTransOrder::connectFilterOps(bool forward, bool isAdapt) {
  // distinguish forward and backward
  auto &ptGraphNodes = forward ? fwPTGraphNodes_ : bwPTGraphNodes_;

  // collect nodes with no filter to visit
  std::queue<std::shared_ptr<PredTransGraphNode>> freeNodes;
  for (auto nodeIt = ptGraphNodes.begin(); nodeIt != ptGraphNodes.end(); ) {
    if ((forward && (*nodeIt)->inPTUnit_.lock()->numFwFilterToVisit_ == 0) ||
        (!forward && (*nodeIt)->inPTUnit_.lock()->numBwFilterToVisit_ == 0)) {
      freeNodes.push(*nodeIt);
      nodeIt = ptGraphNodes.erase(nodeIt);
    } else {
      ++nodeIt;
    }
  }

  // topological ordering
  while (!freeNodes.empty()) {
    auto node = freeNodes.front();
    freeNodes.pop();
    auto inPTUnit = node->inPTUnit_.lock();
    auto outPTUnit = node->outPTUnit_.lock();

    // record the order if need to show
    if (DIST_PRED_TRANS_TYPE == DistPredTransType::ADAPT && metrics::SHOW_DIST_PRED_TRANS_TYPE) {
      adaptDistPTTypes_.order_.emplace_back(std::make_pair(forward, node->step_));
    }

    // prune this step if needed, if not, then connect or exec this step
    bool prune = DIST_PRED_TRANS_TYPE == DistPredTransType::ADAPT && PRUNE_PRED_TRANS &&
                 checkPrune(forward, node);
    if (!prune) {
      // in adaptive exec, make filter ops before connecting
      if(isAdapt) {
        makeOnePairFilterOpsAdapt(forward, node);
      }

      // add ops and connect for this node
      PrePToPTransformerUtil::addPhysicalOps(node->newOps_, transformer_->physicalOps_);
      PrePToPTransformerUtil::connectManyToOneByGroup(inPTUnit->base_->currUpConn_, node->in_);
      PrePToPTransformerUtil::connectManyToOneByGroup(outPTUnit->base_->currUpConn_, node->out_);
      outPTUnit->base_->currUpConn_ = node->filterProbe_;

      // in adaptive exec, exec this pred-trans stage
      if(isAdapt) {
        execPredTranStage(node);
      }
    }

    // update the outgoing neighbors of this node
    if ((forward && ++outPTUnit->numFwFilterVisited_ == outPTUnit->numFwFilterToVisit_) ||
        (!forward && ++outPTUnit->numBwFilterVisited_ == outPTUnit->numBwFilterToVisit_)) {
      for (const auto &outNode: forward ? outPTUnit->fwOutPTNodes_ : outPTUnit->bwOutPTNodes_) {
        freeNodes.push(outNode);
        ptGraphNodes.erase(outNode);
      }
    }
  }

  // throw exception if not all nodes are visited, should not occur
  if (!ptGraphNodes.empty()) {
    throw std::runtime_error(
            fmt::format("{} transfer does not traverse all ptGraphNodes", forward ? "forward" : "backward"));
  }
}

std::shared_ptr<SmallToLargePredTransOrder::PredTransUnit>
SmallToLargePredTransOrder::makePredTransUnit(const std::shared_ptr<PrePhysicalOp> &prePOp,
                                              const POpVec &upConnPOpVec,
                                              const std::vector<std::string> &joinColumns) {
  uint prePOpId = prePOp->getId();
  auto ptUnitIt = ptUnits_.find(prePOpId);
  if (ptUnitIt == ptUnits_.end()) {
    auto initUpConn = PrePToPTransformerUtil::groupByNodeId(upConnPOpVec, transformer_->numNodes_);
    std::vector<POpVec> updatedUpConn;
    bool update = PrePToPTransformerUtil::spreadTableToAllNodes(prePOpId, transformer_->physicalOps_,
                                                                initUpConn, updatedUpConn);
    auto ptUnit = std::make_shared<PredTransUnit>(prePOp, update ? updatedUpConn : initUpConn);
    if (DIST_PRED_TRANS_TYPE == DistPredTransType::ADAPT && PRUNE_PRED_TRANS &&
       prephysical::Util::hasLocalFilter(prePOp, joinColumns)) {
      ptUnit->filterSrc_.addLocal();
    }
    ptUnits_[prePOpId] = ptUnit;
    origUpConnToPTUnit_[prePOpId] = ptUnit->base_;
    return ptUnit;
  } else {
    return ptUnitIt->second;
  }
}

void SmallToLargePredTransOrder::makeOnePairFilterOps(
        bool forward, const std::shared_ptr<JoinOrigin> &joinOrigin,
        uint step, const std::string &hashJoinPredicateStr,
        const std::shared_ptr<PredTransUnit> &leftPTUnit, const std::shared_ptr<PredTransUnit> &rightPTUnit) {
  // distinguish forward and backward
  std::string dirSbl = forward ? "F" : "B";
  const auto &buildColumns = forward ? joinOrigin->leftColumns_ : joinOrigin->rightColumns_;
  const auto &probeColumns = forward ? joinOrigin->rightColumns_ : joinOrigin->leftColumns_;

  POpVec in, out;                         // "in_" and "out_" of PredTransGraphNode
  std::vector<POpVec> filterBuild, filterProbe;    // "bfCreate_" and "bfUse_" of PredTransGraphNode
  POpVec newOps;                          // new ops produced below

  // differentiate single-node and dist pred-trans
  if (transformer_->numNodes_ == 1) {
    makeOnePairFilterOpsSingleNode(dirSbl, step, hashJoinPredicateStr,
                                   buildColumns, probeColumns,
                                   in, out, filterBuild, filterProbe, newOps,
                                   leftPTUnit, rightPTUnit);
  } else {
    makeOnePairFilterOpsDist(dirSbl, step, hashJoinPredicateStr,
                             buildColumns, probeColumns,
                             in, out, filterBuild, filterProbe, newOps,
                             leftPTUnit, rightPTUnit);
  }

#if SHOW_DEBUG_METRICS == true
  // classify created ops into pred-trans phase
  for (const auto &op: newOps) {
    op->setPTPhaseType(metrics::PredTransMetrics::PRED_TRANS_PHASE);
  }
#endif

  // make predicate transfer graph nodes
  if (forward) {
    auto fwPTGraphNode = std::make_shared<PredTransGraphNode>(step, in, out, newOps,
                                                              filterBuild, filterProbe, leftPTUnit, rightPTUnit,
                                                              joinOrigin.get());
    fwPTGraphNodes_.emplace(fwPTGraphNode);
    leftPTUnit->fwOutPTNodes_.emplace_back(fwPTGraphNode);
    ++rightPTUnit->numFwFilterToVisit_;
  } else {
    auto bwPTGraphNode = std::make_shared<PredTransGraphNode>(step, in, out, newOps,
                                                              filterBuild, filterProbe, rightPTUnit, leftPTUnit,
                                                              joinOrigin.get());
    bwPTGraphNodes_.emplace(bwPTGraphNode);
    rightPTUnit->bwOutPTNodes_.emplace_back(bwPTGraphNode);
    ++leftPTUnit->numBwFilterToVisit_;
  }
}

void SmallToLargePredTransOrder::makeOnePairFilterOpsSingleNode(
        const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
        const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
        POpVec& in, POpVec& out,
        std::vector<POpVec>& filterBuild, std::vector<POpVec>& filterProbe,
        POpVec& newOps,
        const std::shared_ptr<PredTransUnit> &leftPTUnit, const std::shared_ptr<PredTransUnit> &rightPTUnit) {
  // In single-node, always using BF as the filter type
  if (transformer_->parallelDegree_ == 1) {
    // single-thread
    auto singleBfCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
            fmt::format("BloomFilterCreate({})<{}>-{}", dirSbl, step, hashJoinPredicateStr),
            std::vector<std::string>{} /*unused*/,
            0,
            buildColumns);
    auto singleBfUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
            fmt::format("BloomFilterUse({})<{}>-{}", dirSbl, step, hashJoinPredicateStr),
            std::vector<std::string>{} /*unused*/,
            0,
            probeColumns);
    singleBfCreate->addBloomFilterUsePOp(singleBfUse);
    singleBfUse->consume(singleBfCreate);
    // add ops
    newOps.insert(newOps.end(), {singleBfCreate, singleBfUse});
    // add ops to "in_", "out_", "bfCreate_", and "bfUse_"
    in = {singleBfCreate};
    out = {singleBfUse};
    filterBuild = {in};
    filterProbe = {out};
#if SHOW_DEBUG_METRICS == true
    if (metrics::SHOW_PRED_TRANS_CS_METRICS) {
      metrics::PredTransCSMetrics::PTCSMetricsInfo buildInfo(
        dirSbl == "F", step,
        leftPTUnit->base_->prePOpId_, rightPTUnit->base_->prePOpId_,
        plan::prephysical::Util::getBaseTableDigest(leftPTUnit->prePOp_),
        plan::prephysical::Util::getBaseTableDigest(rightPTUnit->prePOp_),
        "SINGLE-NODE",
        metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::BUILD);
      metrics::PredTransCSMetrics::PTCSMetricsInfo probeInfo(
        dirSbl == "F", step,
        leftPTUnit->base_->prePOpId_, rightPTUnit->base_->prePOpId_,
        plan::prephysical::Util::getBaseTableDigest(leftPTUnit->prePOp_),
        plan::prephysical::Util::getBaseTableDigest(rightPTUnit->prePOp_),
        "SINGLE-NODE",
        metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::PROBE);
      singleBfCreate->setCollPredTransCSMetrics(buildInfo);
      singleBfUse->setCollPredTransCSMetrics(probeInfo);
    }
#endif
  } else {
    // parallel
    // BF ops
    POpVec singleBfCreateVec, singleBfUseVec;
    for (int i = 0; i < transformer_->parallelDegree_; ++i) {
      singleBfCreateVec.emplace_back(std::make_shared<bloomfilter::BloomFilterCreatePOp>(
              fmt::format("BloomFilterCreate({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
              std::vector<std::string>{} /*unused*/,
              0,
              buildColumns,
              true));
      singleBfUseVec.emplace_back(std::make_shared<bloomfilter::BloomFilterUsePOp>(
              fmt::format("BloomFilterUse({})<{}>-{}-{}", dirSbl, step, hashJoinPredicateStr, i),
              std::vector<std::string>{} /*unused*/,
              0,
              probeColumns));
    }
    newOps.insert(newOps.end(), singleBfCreateVec.begin(), singleBfCreateVec.end());
    newOps.insert(newOps.end(), singleBfUseVec.begin(), singleBfUseVec.end());
    // "GlobalBloomFilterInitPOp" and "SplitPOp"
    std::shared_ptr<PhysicalOp> init = std::make_shared<bloomfilter::GlobalBloomFilterInitPOp>(
            fmt::format("GlobalBloomFilterInit({})<{}>-{}", dirSbl, step, hashJoinPredicateStr),
            std::vector<std::string>{} /*unused*/,
            0,
            buildColumns,
            transformer_->numNodes_);
    PrePToPTransformerUtil::connectOneToMany(init, singleBfCreateVec);
    std::shared_ptr<PhysicalOp> split = std::make_shared<split::SplitPOp>(
            fmt::format("Split({})<{}>-{}", dirSbl, step, hashJoinPredicateStr),
            std::vector<std::string>{} /*unused*/,
            0);
    PrePToPTransformerUtil::connectOneToMany(split, singleBfUseVec);
    newOps.insert(newOps.end(), {init, split});
    // "GlobalBloomFilterFinalizePOp"
    std::shared_ptr<PhysicalOp> finalize = std::make_shared<bloomfilter::GlobalBloomFilterFinalizePOp>(
            fmt::format("GlobalBloomFilterFinalize({})<{}>-{}", dirSbl, step, hashJoinPredicateStr),
            std::vector<std::string>{} /*unused*/,
            0);
    bloomfilter::GlobalBloomFilterFinalizePOp::connectToProducers(finalize, init, singleBfCreateVec);
    bloomfilter::GlobalBloomFilterFinalizePOp::connectToConsumers(finalize, singleBfUseVec, {});
    newOps.emplace_back(finalize);
    // add ops to "in_", "out_", "bfCreate_", and "bfUse_"
    in = {init};
    out = {split};
    filterBuild = {singleBfCreateVec};
    filterProbe = {singleBfUseVec};
#if SHOW_DEBUG_METRICS == true
    if (metrics::SHOW_PRED_TRANS_CS_METRICS) {
      metrics::PredTransCSMetrics::PTCSMetricsInfo buildInfo(
        dirSbl == "F", step,
        leftPTUnit->base_->prePOpId_, rightPTUnit->base_->prePOpId_,
        plan::prephysical::Util::getBaseTableDigest(leftPTUnit->prePOp_),
        plan::prephysical::Util::getBaseTableDigest(rightPTUnit->prePOp_),
        "SINGLE-NODE",
        metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::BUILD);
      metrics::PredTransCSMetrics::PTCSMetricsInfo probeInfo(
        dirSbl == "F", step,
        leftPTUnit->base_->prePOpId_, rightPTUnit->base_->prePOpId_,
        plan::prephysical::Util::getBaseTableDigest(leftPTUnit->prePOp_),
        plan::prephysical::Util::getBaseTableDigest(rightPTUnit->prePOp_),
        "SINGLE-NODE",
        metrics::PredTransCSMetrics::PTCSMetricsBfTimeType::PROBE);
      for (const auto &op: singleBfCreateVec) {
        op->setCollPredTransCSMetrics(buildInfo);
      }
      for (const auto &op: singleBfUseVec) {
        op->setCollPredTransCSMetrics(probeInfo);
      }
      init->setCollPredTransCSMetrics(buildInfo);
      finalize->setCollPredTransCSMetrics(buildInfo);
      split->setCollPredTransCSMetrics(probeInfo);
    }
#endif
  }

  // record the order if need to show
  if (DIST_PRED_TRANS_TYPE == DistPredTransType::ADAPT && metrics::SHOW_DIST_PRED_TRANS_TYPE) {
    bool forward = (dirSbl == "F") ? true : false;
    adaptDistPTTypes_.selections_[{forward, step}] =
      {DistPredTransTypeUtil::toStepDigest(dirSbl, step, hashJoinPredicateStr), "SINGLE-NODE"};
  }
}

void SmallToLargePredTransOrder::addSinkOps(POpVec &upConnPOpVec, std::unordered_set<std::string> &sinkToProduce) {
  POpVec sinkPOpVec;
  for (const auto &upConnPOp: upConnPOpVec) {
    if (upConnPOp->getType() == POpType::ADAPT_SINK) {
      throw std::runtime_error("Connecting two 'AdaptSinkPOp' is not allowed.");
    } else {
      std::string sinkOpName = fmt::format("AdaptSink-{}", upConnPOp->name());
      sinkPOpVec.emplace_back(std::make_shared<adaptive::AdaptSinkPOp>(sinkOpName,
                                                                       std::vector<std::string>{}, /*unused*/
                                                                       upConnPOp->getNodeId()));
      sinkToProduce.emplace(sinkOpName);
    }
  }
  PrePToPTransformerUtil::connectOneToOne(upConnPOpVec, sinkPOpVec);
  PrePToPTransformerUtil::addPhysicalOps(sinkPOpVec, transformer_->physicalOps_);
  upConnPOpVec = sinkPOpVec;
}

void SmallToLargePredTransOrder::addSinkOpsByNode(std::vector<POpVec> &upConnPOpVecByNode,
                                                  std::unordered_set<std::string> &sinkToProduce) {
  std::vector<POpVec> sinkPOpVecByNode;
  for (auto &upConnPOpVec: upConnPOpVecByNode) {
    POpVec sinkPOpVec;
    for (const auto &upConnPOp: upConnPOpVec) {
      if (upConnPOp->getType() == POpType::ADAPT_SINK) {
        throw std::runtime_error("Connecting two 'AdaptSinkPOp' is not allowed.");
      } else {
        std::string sinkOpName = fmt::format("AdaptSink-{}", upConnPOp->name());
        sinkPOpVec.emplace_back(std::make_shared<adaptive::AdaptSinkPOp>(sinkOpName,
                                                                         std::vector<std::string>{}, /*unused*/
                                                                         upConnPOp->getNodeId()));
        sinkToProduce.emplace(sinkOpName);
      }
    }
    PrePToPTransformerUtil::connectOneToOne(upConnPOpVec, sinkPOpVec);
    PrePToPTransformerUtil::addPhysicalOps(sinkPOpVec, transformer_->physicalOps_);
    sinkPOpVecByNode.emplace_back(sinkPOpVec);
  }
  upConnPOpVecByNode = sinkPOpVecByNode;
}

void SmallToLargePredTransOrder::execLocalFilterStage(const JoinOriginSet &joinOrigins) {
  // skip if no join
  if (joinOrigins.empty()) {
    return;
  }

  // construct ops for local filter stage
  std::unordered_set<std::string> sinkToProduce;
  for (const auto &joinOrigin: joinOrigins) {
    // transform the join origin ops
    auto upLeftConnPOpVec = transformer_->transformDfs(joinOrigin->left_);
    auto upRightConnPOpVec = transformer_->transformDfs(joinOrigin->right_);

    // add "AdaptSinkPOp" to separate this exec stage, skip if already visited
    if (upLeftConnPOpVec[0]->getType() != POpType::ADAPT_SINK) {
      addSinkOps(upLeftConnPOpVec, sinkToProduce);
      // update transformation results
      transformer_->prePOpToTransRes_[joinOrigin->left_->getId()] = upLeftConnPOpVec;
    }
    if (upRightConnPOpVec[0]->getType() != POpType::ADAPT_SINK) {
      addSinkOps(upRightConnPOpVec, sinkToProduce);
      // update transformation results
      transformer_->prePOpToTransRes_[joinOrigin->right_->getId()] = upRightConnPOpVec;
    }

    // make or find predicate transfer units when transfer is allowed
    if (joinOrigin->joinType_ == JoinType::FULL) {
      continue;
    }
    auto leftPTUnit = makePredTransUnit(joinOrigin->left_, upLeftConnPOpVec, joinOrigin->leftColumns_);
    auto rightPTUnit = makePredTransUnit(joinOrigin->right_, upRightConnPOpVec, joinOrigin->rightColumns_);

    // make dummy pred-trans graph nodes, which will be filled at runtime later on,
    // i.e., when connecting PT units
    uint step = ptOpIdGen_.fetch_add(1);
    if (joinOrigin->joinType_ != JoinType::RIGHT) {
      auto fwPTGraphNode = std::make_shared<PredTransGraphNode>(step, leftPTUnit, rightPTUnit, joinOrigin.get());
      fwPTGraphNodes_.emplace(fwPTGraphNode);
      leftPTUnit->fwOutPTNodes_.emplace_back(fwPTGraphNode);
      ++rightPTUnit->numFwFilterToVisit_;
    }
    if (joinOrigin->joinType_ != JoinType::LEFT) {
      auto bwPTGraphNode = std::make_shared<PredTransGraphNode>(step, rightPTUnit, leftPTUnit, joinOrigin.get());
      bwPTGraphNodes_.emplace(bwPTGraphNode);
      rightPTUnit->bwOutPTNodes_.emplace_back(bwPTGraphNode);
      ++leftPTUnit->numBwFilterToVisit_;
    }
  }

  // exec local filter stage
  auto adaptExecPlan = std::make_shared<AdaptPhysicalPlan>(transformer_->physicalOps_,
                                                           sinkToProduce,
                                                           std::unordered_map<std::string, bool>{},
                                                           false);
  ((Executor*)(transformer_->executor_))->execNextAdaptStage(transformer_->queryId_, adaptExecPlan);
  transformer_->clear();
}

void SmallToLargePredTransOrder::execPredTranStage(const std::shared_ptr<PredTransGraphNode> &node) {
  // add sink ops for right side
  std::unordered_set<std::string> sinkToProduce;
  addSinkOpsByNode(node->outPTUnit_.lock()->base_->currUpConn_, sinkToProduce);

  // exec this stage
  auto adaptExecPlan = std::make_shared<AdaptPhysicalPlan>(transformer_->physicalOps_,
                                                           sinkToProduce,
                                                           adaptSinkToConsume_,
                                                           false);
  ((Executor*)(transformer_->executor_))->execNextAdaptStage(transformer_->queryId_, adaptExecPlan);
  transformer_->clear();
  adaptSinkToConsume_.clear();
}

void SmallToLargePredTransOrder::makeOnePairFilterOpsAdapt(bool forward,
                                                           const std::shared_ptr<PredTransGraphNode> &node) {
  // before adding new ops, identify sink ops to consume
  auto inPTUnit = node->inPTUnit_.lock();
  auto outPTUnit = node->outPTUnit_.lock();
  for (const auto &opVec: inPTUnit->base_->currUpConn_) {
    for (const auto &op: opVec) {
      adaptSinkToConsume_[op->name()] = true;   // src side sink ops should be preserved
    }
  }
  for (const auto &opVec: outPTUnit->base_->currUpConn_) {
    for (const auto &op: opVec) {
      adaptSinkToConsume_[op->name()] = false;  // dst side sink ops should not be preserved
    }
  }

  // make filter ops
  std::string dirSbl = forward ? "F" : "B";
  POpVec newOps;
  join::HashJoinPredicate hashJoinPredicate(node->joinOrigin_->leftColumns_, node->joinOrigin_->rightColumns_);
  const auto &buildColumns = forward ? node->joinOrigin_->leftColumns_ : node->joinOrigin_->rightColumns_;
  const auto &probeColumns = forward ? node->joinOrigin_->rightColumns_ : node->joinOrigin_->leftColumns_;
  // TODO: adaptively choose the strategy
  makeOnePairFilterOpsSingleNode(dirSbl, node->step_, hashJoinPredicate.toString(),
                                 buildColumns, probeColumns,
                                 node->in_, node->out_,
                                 node->filterBuild_, node->filterProbe_, newOps,
                                 inPTUnit, outPTUnit);
  PrePToPTransformerUtil::addPhysicalOps(newOps, transformer_->physicalOps_);
}

bool SmallToLargePredTransOrder::checkPrune(bool forward, const std::shared_ptr<PredTransGraphNode> &node) {
  bool prune = false;
  auto inPTUnit = node->inPTUnit_.lock();
  auto outPTUnit = node->outPTUnit_.lock();
  // we can prune if current src's predicates are all contained by dst
  // 1) if the step is neither pk->fk nor self->self (self join), src naturally has a pred from itself
  bool hasNaturalSelfFilter;
  // 1a) check pk->fk first
  const auto &left = node->joinOrigin_->left_;
  const auto &right = node->joinOrigin_->right_;
  const auto &leftColumns = node->joinOrigin_->leftColumns_;
  const auto &rightColumns = node->joinOrigin_->rightColumns_;
  const auto &leftScan = plan::prephysical::Util::traceScanOriginWithNoJoinInPath(left);
  const auto &rightScan = plan::prephysical::Util::traceScanOriginWithNoJoinInPath(right);
  if (leftScan == nullptr || rightScan == nullptr) {
    throw std::runtime_error("Fail to trace original scan of join origin");
  }
  bool isFKey = forward ?
                        transformer_->catalogueEntry_->isFKey(rightScan->getTable()->getName(), rightColumns,
                                                              leftScan->getTable()->getName(), leftColumns) :
                        transformer_->catalogueEntry_->isFKey(leftScan->getTable()->getName(), leftColumns,
                                                              rightScan->getTable()->getName(), rightColumns);
  if (isFKey) {
    hasNaturalSelfFilter = false;
  } else {
    // 1b) check "self->self" next (or "self(no filter)->self", a more complete form should be checking
    // "self(partial filter)->self(filter)" instead)
    if (leftColumns == rightColumns) {
      if (PrePhysicalOp::equals(left, right)) {
        hasNaturalSelfFilter = false;
      } else {
        auto inScan = forward ? leftScan : rightScan;
        auto outScan = forward ? rightScan : leftScan;
        if (inScan->getTable()->getName() == outScan->getTable()->getName() && inScan->getPredicate() == nullptr) {
          hasNaturalSelfFilter = false;
        } else {
          hasNaturalSelfFilter = true;
        }
      }
    } else {
      hasNaturalSelfFilter = true;
    }
  }
  // 2) we cannot prune if src naturally has a pred from itself, if not, then
  //    check src's predicates that whether they are all contained by dst
  if (hasNaturalSelfFilter) {
    prune = false;
  } else {
    prune = outPTUnit->filterSrc_.contains(inPTUnit->filterSrc_, !isFKey, prePOpUnions_);
  }
  // based on prune decision
  if (prune) {
    // record this prune action if needed
    if (metrics::SHOW_DIST_PRED_TRANS_TYPE) {
      adaptDistPTTypes_.selections_[{forward, node->step_}].second = (isFKey ? "PRUNED-FK" : "PRUNED-SJ");
    }
  } else {
    // update "filterSrc_" for "outPTUnit"
    // we cannot simply record src id because the src's predicates may be from other tables, i.e. a predicate is
    // transfered by multiple steps, so we need to record the origin of the predicates
    // we also need to record the path of transfer, since a single origin may be transferred following multiple
    // paths, resulting in different selectivities
    // therefore we make a separate class ("FilterSrc") to handle this
    outPTUnit->filterSrc_.addTransferred(inPTUnit->base_->prePOpId_, inPTUnit->filterSrc_, hasNaturalSelfFilter);
  }
  return prune;
}

void SmallToLargePredTransOrder::makePrePOpDisjointSet() {
  std::unordered_map<std::string, std::vector<std::shared_ptr<PrePhysicalOp>>> groups;  // group by scan table name
  for (const auto &ptUnitit: ptUnits_) {
    const auto prePOp = ptUnitit.second->prePOp_;
    prePOpUnions_.insert(prePOp->getId());
    auto scan = prephysical::Util::traceScanOriginWithNoJoinInPath(prePOp);
    if (scan == nullptr) {
      throw std::runtime_error("Fail to trace original scan of join origin");
    }
    const auto &table = scan->getTable()->getName();
    auto groupIt = groups.find(table);
    if (groupIt == groups.end()) {
      groups[table].emplace_back(prePOp);
    } else {
      // check for each existing ptUnit with the same scanning table
      for (const auto &existPrePOp: groupIt->second) {
        if (PrePhysicalOp::equals(prePOp, existPrePOp)) {
          prePOpUnions_.unionSet(prePOp->getId(), existPrePOp->getId());
        }
      }
      groupIt->second.emplace_back(prePOp);
    }
  }
}

void SmallToLargePredTransOrder::showAdaptSelectedDistPTTypes() const {
  if (metrics::SHOW_DIST_PRED_TRANS_TYPE && !adaptDistPTTypes_.selections_.empty()) {
    std::stringstream ss;
    ss << endl << "Adaptively selected dist-PT types |" << endl;
    ss << left << setw(110) << setfill('-') << "" << endl;
    ss << setfill(' ');
    ss << left << setw(75) << "Step";
    ss << left << setw(35) << "Type";
    ss << endl;
    ss << left << setw(110) << setfill('-') << "" << endl;
    ss << setfill(' ');
    for (const auto &key: adaptDistPTTypes_.order_) {
      auto it = adaptDistPTTypes_.selections_.find(key);
      if (it == adaptDistPTTypes_.selections_.end()) {
        continue;
      }
      ss << left << setw(75) << it->second.first;
      ss << left << setw(35) << it->second.second;
      ss << endl;
    }
    ss << left << setw(110) << setfill('-') << "" << endl;
    ss << setfill(' ');
    ss << endl;
    printf("%s", ss.str().c_str());
  }
}

}
