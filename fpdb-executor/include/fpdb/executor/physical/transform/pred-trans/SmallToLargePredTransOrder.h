//
// Created by Yifei Yang on 5/15/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_SMALLTOLARGEPREDTRANSORDER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_SMALLTOLARGEPREDTRANSORDER_H

#include <fpdb/executor/physical/transform/pred-trans/PredTransOrder.h>
#include <fpdb/executor/physical/transform/pred-trans/DistPredTransType.h>
#include <fpdb/executor/physical/transform/pred-trans/FilterSrc.h>

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
    std::shared_ptr<PrePhysicalOp> prePOp_;
    std::vector<std::shared_ptr<PredTransGraphNode>> fwOutPTNodes_, bwOutPTNodes_;  // in/out PT graph nodes
    int numFwFilterToVisit_ = 0, numBwFilterToVisit_ = 0;     // for dependencies
    int numFwFilterVisited_ = 0, numBwFilterVisited_ = 0;     // for dependencies
    FilterSrc filterSrc_;    // record so far received filters, used for pruning

    PredTransUnit(const std::shared_ptr<PrePhysicalOp> &prePOp, const std::vector<POpVec> &upConn):
      base_(std::make_shared<PredTransUnitBase>(prePOp->getId(), upConn)),
      prePOp_(prePOp),
      filterSrc_(prePOp->getId()) {}
  };

  // basic node in predicate transfer dependency graph, i.e. a pair of filter build/probe (e.g., BF create/use)
  // the pairs of filter build/probe will be ordered and connected based on the dependency graph
  struct PredTransGraphNode {
    uint step_;     // identifier for each transfer step, unique in "fw/bwPTGraphNodes_" respectively
    POpVec in_;     // for each node, starting ops of filter build
                    //   e.g., "BloomFilterCreatePOp" for single-thread
                    //         "GlobalBloomFilterInitPOp" for parallel, connected to all "BloomFilterCreatePOp"
    POpVec out_;    // for each node, starting ops of filter probe
                    //   e.g., "BloomFilterUsePOp" for single-thread
                    //         "SplitPOp" for parallel, connected to all "BloomFilterUsePOp"
    POpVec newOps_; // all new ops generated for this transfer step
    std::vector<POpVec> filterBuild_, filterProbe_;       // first level is for each node
    std::weak_ptr<PredTransUnit> inPTUnit_, outPTUnit_;   // the corresponding PT unit of filter build/probe
    JoinOrigin* joinOrigin_;    // the source join origin, used in adaptive pred-trans

    // used in regular static pred-trans
    PredTransGraphNode(uint step,
                       const POpVec &in,
                       const POpVec &out,
                       const POpVec &newOps,
                       const std::vector<POpVec> &filterBuild,
                       const std::vector<POpVec> &filterProbe,
                       const std::shared_ptr<PredTransUnit> &inPTUnit,
                       const std::shared_ptr<PredTransUnit> &outPTUnit,
                       JoinOrigin* joinOrigin):
      step_(step),
      in_(in), out_(out), newOps_(newOps),
      filterBuild_(filterBuild), filterProbe_(filterProbe),
      inPTUnit_(inPTUnit), outPTUnit_(outPTUnit),
      joinOrigin_(joinOrigin) {}

    // used in adaptive pred-trans
    PredTransGraphNode(uint step,
                       const std::shared_ptr<PredTransUnit> &inPTUnit,
                       const std::shared_ptr<PredTransUnit> &outPTUnit,
                       JoinOrigin* joinOrigin):
      step_(step),
      inPTUnit_(inPTUnit), outPTUnit_(outPTUnit),
      joinOrigin_(joinOrigin) {}

    size_t hash() const {
      return step_;
    }

    bool equalTo(const std::shared_ptr<PredTransGraphNode> &other) const {
      return step_ == other->step_;
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

  /**
   * Main steps
   */
  // Main entry, static entry and adaptive entry
  void orderPredTrans(const JoinOriginSet &joinOrigins) override;
  void orderPredTransStatic(const JoinOriginSet &joinOrigins);
  void orderPredTransAdapt(const JoinOriginSet &joinOrigins);

  // Construct pred-trans filters (e.g., join key vals, bloom filters) from pairs of base table joins
  void makeFilterOps(const JoinOriginSet &joinOrigins);

  // Connect filter ops, in a topological order
  void connectFilterOps(bool forward, bool isAdapt = false);

  /**
   * Construct PredTransUnit
   */
  std::shared_ptr<PredTransUnit> makePredTransUnit(const std::shared_ptr<PrePhysicalOp> &prePOp,
                                                   const POpVec &upConnPOpVec,
                                                   const std::vector<std::string> &joinColumns);

  /**
   * Details to construct pred-trans filters
   */
  // Construct pred-trans filters from one pair of base table joins
  void makeOnePairFilterOps(
          bool forward, const std::shared_ptr<JoinOrigin> &joinOrigin,
          uint step, const std::string &hashJoinPredicateStr,
          const std::shared_ptr<PredTransUnit> &leftPTUnit, const std::shared_ptr<PredTransUnit> &rightPTUnit);

  // Single-node pred-trans version
  void makeOnePairFilterOpsSingleNode(
          const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
          const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
          POpVec& in, POpVec& out,
          std::vector<POpVec>& filterBuild, std::vector<POpVec>& filterProbe,
          POpVec& newOps,
          const std::shared_ptr<PredTransUnit> &leftPTUnit, const std::shared_ptr<PredTransUnit> &rightPTUnit);

  // Dist pred-trans version
  void makeOnePairFilterOpsDist(
          const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr,
          const std::vector<std::string> &buildColumns, const std::vector<std::string> &probeColumns,
          POpVec& in, POpVec& out,
          std::vector<POpVec>& filterBuild, std::vector<POpVec>& filterProbe,
          POpVec& newOps,
          const std::shared_ptr<PredTransUnit> &leftPTUnit, const std::shared_ptr<PredTransUnit> &rightPTUnit);

  /**
   * Func used by adaptive pred-trans
   */
  void addSinkOps(POpVec &upConnPOpVec, std::unordered_set<std::string> &sinkToProduce);
  void addSinkOpsByNode(std::vector<POpVec> &upConnPOpVecByNode, std::unordered_set<std::string> &sinkToProduce);
  void execLocalFilterStage(const JoinOriginSet &joinOrigins);
  void execPredTranStage(const std::shared_ptr<PredTransGraphNode> &node);
  void makeOnePairFilterOpsAdapt(bool forward, const std::shared_ptr<PredTransGraphNode> &node);
  bool checkPrune(bool forward, const std::shared_ptr<PredTransGraphNode> &node);
  void makePrePOpDisjointSet();
  void showAdaptSelectedDistPTTypes() const;

  /**
   * extra states maintained during transformation
   */
  // used during predicate transfer, as a dependency graph
  std::unordered_map<uint, std::shared_ptr<PredTransUnit>> ptUnits_;
  std::unordered_set<std::shared_ptr<PredTransGraphNode>, PredTransGraphNodePtrHash, PredTransGraphNodePtrPred>
          fwPTGraphNodes_, bwPTGraphNodes_;

  // used during adaptive exec
  std::unordered_map<std::string, bool> adaptSinkToConsume_;
  struct {
    // here we use a digest to denote step for ease of print
    std::map<std::pair<bool, uint>, std::pair<std::string, std::string>> selections_;
    std::vector<std::pair<bool, uint>> order_;   // used to record the step order
  } adaptDistPTTypes_;

  // used to check whether two prePOps are identical
  fpdb::util::DisjointSet<uint> prePOpUnions_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_SMALLTOLARGEPREDTRANSORDER_H
