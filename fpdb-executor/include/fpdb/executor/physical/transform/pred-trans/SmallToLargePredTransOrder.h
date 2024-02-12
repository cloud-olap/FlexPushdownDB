//
// Created by Yifei Yang on 5/15/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_SMALLTOLARGEPREDTRANSORDER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_SMALLTOLARGEPREDTRANSORDER_H

#include <fpdb/executor/physical/transform/pred-trans/PredTransOrder.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>

namespace fpdb::executor::physical {

class SmallToLargePredTransOrder: public PredTransOrder {

public:
  SmallToLargePredTransOrder(PrePToPTransformerForPredTrans* transformer);
  ~SmallToLargePredTransOrder() override = default;

private:
  struct PredTransUnit;
  struct PredTransGraphNode;

  // basic unit for predicate transfer, extending PredTransUnitBase
  struct PredTransUnit {
    std::shared_ptr<PredTransUnitBase> base_;
    std::vector<std::shared_ptr<PredTransGraphNode>> fwOutPTNodes_, bwOutPTNodes_;  // in/out PT graph nodes
    int numFwBFUseToVisit_ = 0, numBwBFUseToVisit_ = 0;     // for dependencies
    int numFwBFUseVisited_ = 0, numBwBFUseVisited_ = 0;     // for dependencies

    PredTransUnit(uint prePOpId, const std::shared_ptr<PhysicalOp> &upConnOp):
      base_(std::make_shared<PredTransUnitBase>(prePOpId, upConnOp)) {}
  };

  struct PredTransUnitPtrHash {
    inline size_t operator()(const std::shared_ptr<PredTransUnit> &ptUnit) const {
      return ptUnit->base_->hash();
    }
  };

  struct PredTransUnitPtrPred {
    inline bool operator()(const std::shared_ptr<PredTransUnit> &lhs, const std::shared_ptr<PredTransUnit> &rhs) const {
      return lhs->base_->equalTo(rhs->base_);
    }
  };

  // basic node in predicate transfer dependency graph, i.e. a pair of BF create/use
  // the pairs of BF create/use will be ordered and connected based on the dependency graph
  struct PredTransGraphNode {
    std::shared_ptr<bloomfilter::BloomFilterCreatePOp> bfCreate_;
    std::shared_ptr<bloomfilter::BloomFilterUsePOp> bfUse_;
    std::weak_ptr<PredTransUnit> bfCreatePTUnit_, bfUsePTUnit_;   // the corresponding PT unit of BF create/use

    PredTransGraphNode(const std::shared_ptr<bloomfilter::BloomFilterCreatePOp> &bfCreate,
                       const std::shared_ptr<bloomfilter::BloomFilterUsePOp> &bfUse,
                       const std::shared_ptr<PredTransUnit> &bfCreatePTUnit,
                       const std::shared_ptr<PredTransUnit> &bfUsePTUnit):
            bfCreate_(bfCreate), bfUse_(bfUse), bfCreatePTUnit_(bfCreatePTUnit), bfUsePTUnit_(bfUsePTUnit) {}

    size_t hash() const {
      return std::hash<std::string>()(bfCreate_->name() + " - " + bfUse_->name());
    }

    bool equalTo(const std::shared_ptr<PredTransGraphNode> &other) const {
      return bfCreate_->name() == other->bfCreate_->name() && bfUse_->name() == other->bfUse_->name();
    }
  };

  struct PredTransGraphNodePtrHash {
    inline size_t operator()(const std::shared_ptr<PredTransGraphNode> &node) const {
      return node->hash();
    }
  };

  struct PredTransGraphNodePtrPred {
    inline bool operator()(const std::shared_ptr<PredTransGraphNode> &lhs,
                           const std::shared_ptr<PredTransGraphNode> &rhs) const {
      return lhs->equalTo(rhs);
    }
  };

  // Main entry
  void orderPredTrans(const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash,
          JoinOriginPtrPred> &joinOrigins) override;

  // Construct bloom filter ops from pairs of base table joins
  void makeBloomFilterOps(
          const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins);

  // Connect bloom filter ops created above, in a topological order
  void connectFwBloomFilterOps();
  void connectBwBloomFilterOps();

  /**
   * extra states maintained during transformation
   */
  // used during predicate transfer, as a dependency graph
  std::unordered_set<std::shared_ptr<PredTransUnit>, PredTransUnitPtrHash, PredTransUnitPtrPred> ptUnits_;
  std::unordered_set<std::shared_ptr<PredTransGraphNode>, PredTransGraphNodePtrHash, PredTransGraphNodePtrPred>
          fwPTGraphNodes_, bwPTGraphNodes_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_SMALLTOLARGEPREDTRANSORDER_H
