//
// Created by Yifei Yang on 5/15/23.
//

#include <fpdb/executor/physical/transform/pred-trans/BFSPredTransOrder.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinArrowPOp.h>
#include <queue>

namespace fpdb::executor::physical {

BFSPredTransOrder::BFSPredTransOrder(PrePToPTransformerForPredTrans* transformer,
                                     bool isYannakakis):
        PredTransOrder(PredTransOrderType::BFS, transformer),
        isYannakakis_(isYannakakis) {}

void BFSPredTransOrder::orderPredTrans(
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  // make pred-trans units
  makePTUnits(joinOrigins);

  // order pred-trans by a BFS search
  bfsSearch();

  // connect pairs ptUnits by pred-trans operators (i.e. bloom filter / semi-join)
  connectPTUnits();

  // update transformation results
  updateTransRes();

#if SHOW_DEBUG_METRICS == true
  // collect predicate transfer metrics
  for (const auto &ptUnit: ptUnits_) {
    auto currConnOp = ptUnit->base_->currUpConnOp_;
    uint prePOpId = ptUnit->base_->prePOpId_;
    currConnOp->setCollPredTransMetrics(prePOpId, metrics::PredTransMetrics::PTMetricsUnitType::PRED_TRANS);
  }
#endif
}

void BFSPredTransOrder::makePTUnits(
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  // traverse join edges
  for (const auto &joinOrigin: joinOrigins) {
    // transform the join origin ops
    auto upLeftConnPOps = transformer_->transformDfs(joinOrigin->left_);
    auto upRightConnPOps = transformer_->transformDfs(joinOrigin->right_);

    // FIXME: currently only support single-partition-table / single-thread-processing
    if (upLeftConnPOps.size() != 1 || upRightConnPOps.size() != 1) {
      throw std::runtime_error("Currently only support single-partition-table / single-thread-processing");
    }
    auto upLeftConnPOp = upLeftConnPOps[0];
    auto upRightConnPOp = upRightConnPOps[0];

    // cannot transfer on FULL joins
    if (joinOrigin->joinType_ == JoinType::FULL) {
      continue;
    }

    // make or find predicate transfer units
    auto leftPTUnit = std::make_shared<PredTransUnit>(joinOrigin->left_->getId(),
                                                      upLeftConnPOp,
                                                      joinOrigin->left_->getRowCount());
    auto ptUnitIt = ptUnits_.find(leftPTUnit);
    if (ptUnitIt == ptUnits_.end()) {
      ptUnits_.emplace(leftPTUnit);
      origUpConnOpToPTUnit_[upLeftConnPOp->name()] = leftPTUnit->base_;
    } else {
      leftPTUnit = *ptUnitIt;
    }
    auto rightPTUnit = std::make_shared<PredTransUnit>(joinOrigin->right_->getId(),
                                                       upRightConnPOp,
                                                       joinOrigin->right_->getRowCount());
    ptUnitIt = ptUnits_.find(rightPTUnit);
    if (ptUnitIt == ptUnits_.end()) {
      ptUnits_.emplace(rightPTUnit);
      origUpConnOpToPTUnit_[upRightConnPOp->name()] = rightPTUnit->base_;
    } else {
      rightPTUnit = *ptUnitIt;
    }

    // add neighbors
    TransferDir leftTransferDir = TransferDir::BOTH;
    TransferDir rightTransferDir = TransferDir::BOTH;
    if (joinOrigin->joinType_ == JoinType::LEFT) {
      leftTransferDir = TransferDir::FORWARD_ONLY;
      rightTransferDir = TransferDir::BACKWARD_ONLY;
    } else if (joinOrigin->joinType_ == JoinType::RIGHT) {
      leftTransferDir = TransferDir::BACKWARD_ONLY;
      rightTransferDir = TransferDir::FORWARD_ONLY;
    }
    leftPTUnit->neighbors_.emplace_back(std::make_shared<PredTransNeighbor>(
            rightPTUnit, leftTransferDir, joinOrigin->leftColumns_, joinOrigin->rightColumns_));
    rightPTUnit->neighbors_.emplace_back(std::make_shared<PredTransNeighbor>(
            leftPTUnit, rightTransferDir, joinOrigin->rightColumns_, joinOrigin->leftColumns_));
  }
}

void BFSPredTransOrder::bfsSearch() {
  // skip if there is no joins
  if (ptUnits_.empty()) {
    return;
  }
  auto ptUnitsToVisit = ptUnits_;        // use a copy since we erase elements below
  std::shared_ptr<PredTransUnit> root;

  // keep doing bfs search until all are visited, one bfs search may not visit all nodes, since there may be multiple
  // connected components in the join graph, i.e., some joins are not equi-joins so no transfer can happen,
  // in such case we need to start another bfs search on the rest nodes until all are visited
  while (!ptUnitsToVisit.empty()) {
    // get the root node
    if (isYannakakis_) {
      // Yannakakis does not specify the root so we just pick the first one
      root = *ptUnitsToVisit.begin();
    } else {
      // in pred-trans, we specify the root as the ptUnit with the largest table
      double rootRowCount_ = -1;
      for (const auto &ptUnit: ptUnitsToVisit) {
        if (ptUnit->rowCount_ > rootRowCount_) {
          root = ptUnit;
          rootRowCount_ = ptUnit->rowCount_;
        }
      }
    }
    // actual bfs happens here
    doBfsSearch(ptUnitsToVisit, root);
  }
}

void BFSPredTransOrder::doBfsSearch(PTUnitSet &ptUnitsToVisit, const std::shared_ptr<PredTransUnit> &root) {
  std::queue<std::shared_ptr<PredTransUnit>> bfsQueue;
  bfsQueue.push(root);
  ptUnitsToVisit.erase(root);

  // BFS search
  while (!bfsQueue.empty()) {
    const auto &ptUnit = bfsQueue.front();
    bfsQueue.pop();
    for (const auto &neighbor: ptUnit->neighbors_) {
      auto nextPtUnit = neighbor->ptUnit_.lock();
      if (ptUnitsToVisit.find(nextPtUnit) == ptUnitsToVisit.end()) {
        // this node has been visited
        continue;
      }
      // get whether the forward or backward transfer is doable, or both are
      bool forward = neighbor->transferDir_ == TransferDir::BOTH ||
                     neighbor->transferDir_ == TransferDir::FORWARD_ONLY;
      bool backward = neighbor->transferDir_ == TransferDir::BOTH ||
                      neighbor->transferDir_ == TransferDir::BACKWARD_ONLY;
      forwardOrder_.push(std::make_shared<BFSPredTransPair>(nextPtUnit, ptUnit,
                                                            neighbor->rightColumns_, neighbor->leftColumns_,
                                                            backward, forward));
      bfsQueue.push(nextPtUnit);
      ptUnitsToVisit.erase(nextPtUnit);
    }
  }
}

void BFSPredTransOrder::connectPTUnits() {
  connectPTUnits(true);     // forward
  connectPTUnits(false);    // backward
}

void BFSPredTransOrder::connectPTUnits(bool isForward) {
  auto &orderToVisit = isForward ? forwardOrder_ : backwardOrder_;
  std::string dirNameTag = isForward ? "F" : "B";
  while (!orderToVisit.empty()) {
    auto ptPair = orderToVisit.top();
    orderToVisit.pop();
    if (ptPair->forward_) {
      join::HashJoinPredicate hashJoinPredicate(ptPair->srcColumns_, ptPair->tgtColumns_);
      const auto &hashJoinPredicateStr = hashJoinPredicate.toString();
      uint ptOpId = ptOpIdGen_.fetch_add(1);
      if (isYannakakis_) {
        // semi-join for Yannakakis
        std::shared_ptr<PhysicalOp> hashJoin = std::make_shared<join::HashJoinArrowPOp>(
                fmt::format("HashJoinArrow({})<{}>-{}", dirNameTag, ptOpId, hashJoinPredicateStr),
                ptPair->tgtPTUnit_->base_->origUpConnOp_->getProjectColumnNames(),
                0,
                hashJoinPredicate,
                JoinType::RIGHT_SEMI);
        // connect and add ops
        std::static_pointer_cast<join::HashJoinArrowPOp>(hashJoin)
                ->addBuildProducer(ptPair->srcPTUnit_->base_->currUpConnOp_);
        std::static_pointer_cast<join::HashJoinArrowPOp>(hashJoin)
                ->addProbeProducer(ptPair->tgtPTUnit_->base_->currUpConnOp_);
        ptPair->srcPTUnit_->base_->currUpConnOp_->produce(hashJoin);
        ptPair->tgtPTUnit_->base_->currUpConnOp_->produce(hashJoin);
        PrePToPTransformerUtil::addPhysicalOps({hashJoin}, transformer_->physicalOps_);
        // update currUpConnOp for ptUnit
        ptPair->tgtPTUnit_->base_->currUpConnOp_ = hashJoin;
      } else {
        // bloom filter for pred-trans
        std::shared_ptr<PhysicalOp> bfCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
                fmt::format("BloomFilterCreate({})<{}>-{}", dirNameTag, ptOpId, hashJoinPredicateStr),
                std::vector<std::string>{},
                0,
                ptPair->srcColumns_);
        std::shared_ptr<PhysicalOp> bfUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
                fmt::format("BloomFilterUse({})<{}>-{}", dirNameTag, ptOpId, hashJoinPredicateStr),
                std::vector<std::string>{},
                0,
                ptPair->tgtColumns_);
        // connect and add ops
        std::static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(bfCreate)->addBloomFilterUsePOp(bfUse);
        bfUse->consume(bfCreate);
        PrePToPTransformerUtil::connectOneToOne(ptPair->srcPTUnit_->base_->currUpConnOp_, bfCreate);
        PrePToPTransformerUtil::connectOneToOne(ptPair->tgtPTUnit_->base_->currUpConnOp_, bfUse);
        PrePToPTransformerUtil::addPhysicalOps({bfCreate, bfUse}, transformer_->physicalOps_);
        // update currUpConnOp for ptUnit
        ptPair->tgtPTUnit_->base_->currUpConnOp_ = bfUse;
      }
    }

    // also construct the reversed order during the forward visit, for backward transfer
    if (isForward) {
      backwardOrder_.push(std::make_shared<BFSPredTransPair>(ptPair->tgtPTUnit_, ptPair->srcPTUnit_,
                                                             ptPair->tgtColumns_, ptPair->srcColumns_,
                                                             ptPair->backward_, ptPair->forward_));
    }
  }
}

}
