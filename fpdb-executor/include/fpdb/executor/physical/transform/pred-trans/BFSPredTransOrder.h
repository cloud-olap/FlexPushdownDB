//
// Created by Yifei Yang on 5/15/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_BFSPREDTRANSORDER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_BFSPREDTRANSORDER_H

#include <fpdb/executor/physical/transform/pred-trans/PredTransOrder.h>
#include <stack>

namespace fpdb::executor::physical {

class BFSPredTransOrder: public PredTransOrder {

public:
  BFSPredTransOrder(PrePToPTransformerForPredTrans* transformer,
                    bool isYannakakis);
  ~BFSPredTransOrder() override = default;

private:
  enum TransferDir {
    FORWARD_ONLY,
    BACKWARD_ONLY,
    BOTH
  };

  struct PredTransUnit;
  struct PredTransNeighbor;     // neighbors in the join graph

  struct PredTransUnit {
    std::shared_ptr<PredTransUnitBase> base_;
    double rowCount_;
    std::vector<std::shared_ptr<PredTransNeighbor>> neighbors_;          // neighbors

    PredTransUnit(uint prePOpId, const std::shared_ptr<PhysicalOp> &upConnOp, double rowCount):
      base_(std::make_shared<PredTransUnitBase>(prePOpId, upConnOp)),
      rowCount_(rowCount) {}
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

  using PTUnitSet = std::unordered_set<std::shared_ptr<PredTransUnit>, PredTransUnitPtrHash, PredTransUnitPtrPred>;

  struct PredTransNeighbor {
    std::weak_ptr<PredTransUnit> ptUnit_;       // neighbor ptUnit
    TransferDir transferDir_;                   // transfer dir from source to neighbor (this)
    std::vector<std::string> leftColumns_;      // join columns in source
    std::vector<std::string> rightColumns_;     // join columns in neighbor (this)

    PredTransNeighbor(const std::shared_ptr<PredTransUnit> &ptUnit, TransferDir transferDir,
                      const std::vector<std::string> &leftColumns, const std::vector<std::string> &rightColumns):
      ptUnit_(ptUnit), transferDir_(transferDir),
      leftColumns_(leftColumns), rightColumns_(rightColumns) {}
  };

  struct BFSPredTransPair {
    std::shared_ptr<PredTransUnit> srcPTUnit_;
    std::shared_ptr<PredTransUnit> tgtPTUnit_;
    std::vector<std::string> srcColumns_;
    std::vector<std::string> tgtColumns_;
    bool forward_;
    bool backward_;

    BFSPredTransPair(const std::shared_ptr<PredTransUnit> &srcPTUnit,
                     const std::shared_ptr<PredTransUnit> &tgtPTUnit,
                     const std::vector<std::string> &srcColumns,
                     const std::vector<std::string> &tgtColumns,
                     bool forward,
                     bool backward):
      srcPTUnit_(srcPTUnit), tgtPTUnit_(tgtPTUnit),
      srcColumns_(srcColumns), tgtColumns_(tgtColumns),
      forward_(forward), backward_(backward) {}
  };

  // main entry
  void orderPredTrans(const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash,
          JoinOriginPtrPred> &joinOrigins) override;

  // construct pred-trans units
  void makePTUnits(
          const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins);

  // get an order of pred-trans between ptUnits
  void bfsSearch();
  void doBfsSearch(PTUnitSet &ptUnitsToVisit, const std::shared_ptr<PredTransUnit> &root);

  // connect pairs ptUnits by pred-trans operators (i.e. bloom filter / semi-join)
  void connectPTUnits();
  void connectPTUnits(bool isForward);

  bool isYannakakis_;     // whether this is Yannakakis algorithm

  /**
   * extra states maintained during transformation
   */
  // pred-trans units
  PTUnitSet ptUnits_;

  // BFS ordering result, the order is the reverse of BFS search so use a stack here
  // forward is constructed during BFS ordering, backward is construct during the visit of the forward one
  std::stack<std::shared_ptr<BFSPredTransPair>> forwardOrder_;
  std::stack<std::shared_ptr<BFSPredTransPair>> backwardOrder_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_BFSPREDTRANSORDER_H
