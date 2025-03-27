//
// Created by Yifei Yang on 12/27/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_LIPPREDTRANSORDER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_LIPPREDTRANSORDER_H

#include <fpdb/executor/physical/transform/pred-trans/PredTransOrder.h>

namespace fpdb::executor::physical {

/**
 * LIP-style predicate transfer
 */
class LIPPredTransOrder: public PredTransOrder {
public:
  LIPPredTransOrder(PrePToPTransformerForPredTrans* transformer);
  ~LIPPredTransOrder() override = default;

private:
  // Used to describe a join substree with a star schema, which should have one fact table and
  // any number of dimension tables (can be zero)
  struct StarDimension {
    std::shared_ptr<PrePhysicalOp> dimensionOp_;
    std::vector<std::string> factColumns_, dimensionColumns_;
    std::string hashJoinPredicateStr_;

    StarDimension(const std::shared_ptr<PrePhysicalOp> &dimensionOp,
                  const std::vector<std::string> &factColumns,
                  const std::vector<std::string> &dimensionColumns,
                  const std::string &hashJoinPredicateStr):
      dimensionOp_(dimensionOp),
      factColumns_(factColumns),
      dimensionColumns_(dimensionColumns),
      hashJoinPredicateStr_(hashJoinPredicateStr) {}
  };

  struct StarSubtree {
    std::shared_ptr<PrePhysicalOp> factOp_;
    std::vector<StarDimension> dimensions_;

    StarSubtree(const std::shared_ptr<PrePhysicalOp> &factOp):
      factOp_(factOp) {}
  };

  // basic unit for predicate transfer, extending PredTransUnitBase
  struct PredTransUnit {
    std::shared_ptr<PredTransUnitBase> base_;
    std::shared_ptr<PrePhysicalOp> prePOp_;

    PredTransUnit(const std::shared_ptr<PrePhysicalOp> &prePOp, const std::vector<POpVec> &upConn):
      base_(std::make_shared<PredTransUnitBase>(prePOp->getId(), upConn)),
      prePOp_(prePOp) {}
  };

  // Main entry
  void orderPredTrans(const JoinOriginSet &joinOrigins) override;

  // Map to get all direct joins for a particular base table op
  // Make sure the pair of two prePOpId of the key are ordered
  using JoinOriginKey = std::pair<uint, uint>;
  static JoinOriginKey makeJoinOriginKey(uint prePOpId1, uint prePOpId2);
  struct JoinOriginKeyHash {
    size_t operator()(const JoinOriginKey &key) const {
      return (((size_t) key.first) << 32) & key.second;
    }
  };
  using JoinOriginMap = std::unordered_map<JoinOriginKey, std::shared_ptr<JoinOrigin>, JoinOriginKeyHash>;
  static JoinOriginMap generateJoinOriginMap(const JoinOriginSet &joinOrigins);

  // Expand LIP-style PT on all the left-deep subtrees with star schemas
  void expandLIP(const JoinOriginMap &joinOriginMap,
                 const std::shared_ptr<PrePhysicalPlan> &prePhysicalPlan);

  // Return a `StarSubtree` if the current expanded one can potentially be further expanded in the upper level,
  // or nullptr otherwise
  std::shared_ptr<StarSubtree> expandLIP_DFS(const JoinOriginMap &joinOriginMap,
                                             const std::shared_ptr<PrePhysicalOp> &op);

  // Make and connect Bloom filter ops based on constructed `lipSubtrees_`
  void makeAndConnectFilterOps();

  // Make and connect Bloom filter ops for one `lipSubtree`
  void makeAndConnectOneSubtreeFilterOps(const std::shared_ptr<StarSubtree> &lipSubtree);

  // Make and connect Bloom filter ops for one pair of src and dst tables
  void makeAndConnectOnePairFilterOps(uint lipFilterId,
                                      const std::shared_ptr<PredTransUnit> &srcPTUnit,
                                      const std::shared_ptr<PredTransUnit> &dstPTUnit,
                                      const std::string &hashJoinPredicateStr,
                                      const std::vector<std::string> &buildColumns,
                                      const std::vector<std::string> &probeColumns);

  // Make or find `ptUnit` for an input table
  std::shared_ptr<PredTransUnit> makePredTransUnit(const std::shared_ptr<PrePhysicalOp> &prePOp);

  /**
   * States maintained during transformation
   */
  // Star substrees that are qualified for LIP-style predicate transfer
  std::vector<std::shared_ptr<StarSubtree>> lipSubtrees_;

  // A `ptUnit` is a input table
  std::unordered_map<uint, std::shared_ptr<PredTransUnit>> ptUnits_;
  std::unordered_set<uint> dstPtUnits_;   // `prePOpId_` of `ptUnits_` which are dst of LIP filters
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_LIPPREDTRANSORDER_H
