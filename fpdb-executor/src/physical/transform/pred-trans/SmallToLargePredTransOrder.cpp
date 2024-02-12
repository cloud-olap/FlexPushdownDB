//
// Created by Yifei Yang on 5/15/23.
//

#include <fpdb/executor/physical/transform/pred-trans/SmallToLargePredTransOrder.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinPredicate.h>

namespace fpdb::executor::physical {

SmallToLargePredTransOrder::SmallToLargePredTransOrder(PrePToPTransformerForPredTrans* transformer):
  PredTransOrder(PredTransOrderType::SMALL_TO_LARGE, transformer) {}

void SmallToLargePredTransOrder::orderPredTrans(
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  // create bloom filter ops (both forward and backward)
  makeBloomFilterOps(joinOrigins);

  // connect forward bloom filter ops
  connectFwBloomFilterOps();

  // connect backward bloom filter ops
  connectBwBloomFilterOps();

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

void SmallToLargePredTransOrder::makeBloomFilterOps(
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
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
    auto leftPTUnit = std::make_shared<PredTransUnit>(joinOrigin->left_->getId(), upLeftConnPOp);
    auto ptUnitIt = ptUnits_.find(leftPTUnit);
    if (ptUnitIt == ptUnits_.end()) {
      ptUnits_.emplace(leftPTUnit);
      origUpConnOpToPTUnit_[upLeftConnPOp->name()] = leftPTUnit->base_;
    } else {
      leftPTUnit = *ptUnitIt;
    }
    auto rightPTUnit = std::make_shared<PredTransUnit>(joinOrigin->right_->getId(), upRightConnPOp);
    ptUnitIt = ptUnits_.find(rightPTUnit);
    if (ptUnitIt == ptUnits_.end()) {
      ptUnits_.emplace(rightPTUnit);
      origUpConnOpToPTUnit_[upRightConnPOp->name()] = rightPTUnit->base_;
    } else {
      rightPTUnit = *ptUnitIt;
    }

    // make bloom filter ops
    join::HashJoinPredicate hashJoinPredicate(joinOrigin->leftColumns_, joinOrigin->rightColumns_);
    const auto &hashJoinPredicateStr = hashJoinPredicate.toString();
    uint bfId = ptOpIdGen_.fetch_add(1);

    // forward bloom filter, blocked by right joins
    if (joinOrigin->joinType_ != JoinType::RIGHT) {
      auto fwBFCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
              fmt::format("BloomFilterCreate(F)<{}>-{}", bfId, hashJoinPredicateStr),
              std::vector<std::string>{},
              0,
              joinOrigin->leftColumns_);
      auto fwBFUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
              fmt::format("BloomFilterUse(F)<{}>-{}", bfId, hashJoinPredicateStr),
              std::vector<std::string>{},
              0,
              joinOrigin->rightColumns_);
      fwBFCreate->addBloomFilterUsePOp(fwBFUse);
      fwBFUse->consume(fwBFCreate);

      // add ops
      PrePToPTransformerUtil::addPhysicalOps({fwBFCreate, fwBFUse}, transformer_->physicalOps_);

      // make predicate transfer graph nodes
      auto fwPTGraphNode = std::make_shared<PredTransGraphNode>(fwBFCreate, fwBFUse, leftPTUnit, rightPTUnit);
      fwPTGraphNodes_.emplace(fwPTGraphNode);
      leftPTUnit->fwOutPTNodes_.emplace_back(fwPTGraphNode);
      ++rightPTUnit->numFwBFUseToVisit_;
    }

    // backward bloom filter, blocked by left joins
    if (joinOrigin->joinType_ != JoinType::LEFT) {
      auto bwBFCreate = std::make_shared<bloomfilter::BloomFilterCreatePOp>(
              fmt::format("BloomFilterCreate(B)<{}>-{}", bfId, hashJoinPredicateStr),
              std::vector<std::string>{},
              0,
              joinOrigin->rightColumns_);
      auto bwBFUse = std::make_shared<bloomfilter::BloomFilterUsePOp>(
              fmt::format("BloomFilterUse(B)<{}>-{}", bfId, hashJoinPredicateStr),
              std::vector<std::string>{},
              0,
              joinOrigin->leftColumns_);
      bwBFCreate->addBloomFilterUsePOp(bwBFUse);
      bwBFUse->consume(bwBFCreate);

      // add ops
      PrePToPTransformerUtil::addPhysicalOps({bwBFCreate, bwBFUse}, transformer_->physicalOps_);

      // make predicate transfer graph nodes
      auto bwPTGraphNode = std::make_shared<PredTransGraphNode>(bwBFCreate, bwBFUse, rightPTUnit, leftPTUnit);
      bwPTGraphNodes_.emplace(bwPTGraphNode);
      rightPTUnit->bwOutPTNodes_.emplace_back(bwPTGraphNode);
      ++leftPTUnit->numBwBFUseToVisit_;
    }
  }
}

void SmallToLargePredTransOrder::connectFwBloomFilterOps() {
  // collect nodes with no bfUse to visit
  std::queue<std::shared_ptr<PredTransGraphNode>> freeNodes;
  for (auto nodeIt = fwPTGraphNodes_.begin(); nodeIt != fwPTGraphNodes_.end(); ) {
    if ((*nodeIt)->bfCreatePTUnit_.lock()->numFwBFUseToVisit_ == 0) {
      freeNodes.push(*nodeIt);
      nodeIt = fwPTGraphNodes_.erase(nodeIt);
    } else {
      ++nodeIt;
    }
  }

  // topological ordering
  while (!freeNodes.empty()) {
    const auto &node = freeNodes.front();
    freeNodes.pop();

    // connect for this node
    auto bfCreatePTUnit = node->bfCreatePTUnit_.lock();
    auto bfUsePTUnit = node->bfUsePTUnit_.lock();
    std::shared_ptr<PhysicalOp> bfCreate = node->bfCreate_;
    std::shared_ptr<PhysicalOp> bfUse = node->bfUse_;
    PrePToPTransformerUtil::connectOneToOne(bfCreatePTUnit->base_->currUpConnOp_, bfCreate);
    PrePToPTransformerUtil::connectOneToOne(bfUsePTUnit->base_->currUpConnOp_, bfUse);
    bfUsePTUnit->base_->currUpConnOp_ = bfUse;
    ++bfUsePTUnit->numFwBFUseVisited_;

    // update the outgoing neighbors of this node
    if (bfUsePTUnit->numFwBFUseVisited_ == bfUsePTUnit->numFwBFUseToVisit_) {
      for (const auto &outNode: bfUsePTUnit->fwOutPTNodes_) {
        freeNodes.push(outNode);
        fwPTGraphNodes_.erase(outNode);
      }
    }
  }

  // throw exception when there is a cycle
  if (!fwPTGraphNodes_.empty()) {
    throw std::runtime_error("The join origins (base table joins) contain cycles (forward predicate transfer)");
  }
}

void SmallToLargePredTransOrder::connectBwBloomFilterOps() {
  // collect nodes with no bfUse to visit
  std::queue<std::shared_ptr<PredTransGraphNode>> freeNodes;
  for (auto nodeIt = bwPTGraphNodes_.begin(); nodeIt != bwPTGraphNodes_.end(); ) {
    if ((*nodeIt)->bfCreatePTUnit_.lock()->numBwBFUseToVisit_ == 0) {
      freeNodes.push(*nodeIt);
      nodeIt = bwPTGraphNodes_.erase(nodeIt);
    } else {
      ++nodeIt;
    }
  }

  // topological ordering
  while (!freeNodes.empty()) {
    const auto &node = freeNodes.front();
    freeNodes.pop();

    // connect for this node
    auto bfCreatePTUnit = node->bfCreatePTUnit_.lock();
    auto bfUsePTUnit = node->bfUsePTUnit_.lock();
    std::shared_ptr<PhysicalOp> bfCreate = node->bfCreate_;
    std::shared_ptr<PhysicalOp> bfUse = node->bfUse_;
    PrePToPTransformerUtil::connectOneToOne(bfCreatePTUnit->base_->currUpConnOp_, bfCreate);
    PrePToPTransformerUtil::connectOneToOne(bfUsePTUnit->base_->currUpConnOp_, bfUse);
    bfUsePTUnit->base_->currUpConnOp_ = bfUse;
    ++bfUsePTUnit->numBwBFUseVisited_;

    // update the outgoing neighbors of this node
    if (bfUsePTUnit->numBwBFUseVisited_ == bfUsePTUnit->numBwBFUseToVisit_) {
      for (const auto &outNode: bfUsePTUnit->bwOutPTNodes_) {
        freeNodes.push(outNode);
        bwPTGraphNodes_.erase(outNode);
      }
    }
  }

  // throw exception when there is a cycle
  if (!bwPTGraphNodes_.empty()) {
    throw std::runtime_error("The join origins (base table joins) contain cycles (backward predicate transfer)");
  }
}

}
