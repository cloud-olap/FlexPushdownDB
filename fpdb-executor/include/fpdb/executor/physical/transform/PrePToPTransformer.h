//
// Created by Yifei Yang on 11/20/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMER_H

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <fpdb/executor/physical/join/DistJoinType.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/plan/prephysical/PrePhysicalPlan.h>
#include <fpdb/plan/prephysical/SortPrePOp.h>
#include <fpdb/plan/prephysical/LimitSortPrePOp.h>
#include <fpdb/plan/prephysical/AggregatePrePOp.h>
#include <fpdb/plan/prephysical/GroupPrePOp.h>
#include <fpdb/plan/prephysical/ProjectPrePOp.h>
#include <fpdb/plan/prephysical/FilterPrePOp.h>
#include <fpdb/plan/prephysical/HashJoinPrePOp.h>
#include <fpdb/plan/prephysical/NestedLoopJoinPrePOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/UnionAllPrePOp.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/plan/Mode.h>
#include <fpdb/catalogue/CatalogueEntry.h>
#include <fpdb/catalogue/obj-store/ObjStoreConnector.h>

using namespace fpdb::plan;
using namespace fpdb::plan::prephysical;
using namespace fpdb::plan::prephysical::separable;
using namespace fpdb::catalogue;
using namespace fpdb::catalogue::obj_store;
using POpVec = std::vector<std::shared_ptr<PhysicalOp>>;

namespace fpdb::executor::physical {

enum class PrePToPTransformerType {
  REGULAR,
  PRED_TRANS
};

class PrePToPTransformer {

public:
  static shared_ptr<PhysicalPlan> transform(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                                            const shared_ptr<CatalogueEntry> &catalogueEntry,
                                            const shared_ptr<ObjStoreConnector> &objStoreConnector,
                                            const shared_ptr<Mode> &mode,
                                            int parallelDegree,
                                            int numNodes,
                                            // used by adapt exec
                                            long queryId = 0,
                                            bool isDistributed = false,
                                            void* executor = nullptr);

  PrePToPTransformer(const shared_ptr<PrePhysicalPlan> &prePhysicalPlan,
                     const shared_ptr<CatalogueEntry> &catalogueEntry,
                     const shared_ptr<ObjStoreConnector> &objStoreConnector,
                     const shared_ptr<Mode> &mode,
                     int parallelDegree,
                     int numNodes,
                     // used by adapt exec
                     long queryId,
                     bool isDistributed,
                     void* executor);
  virtual ~PrePToPTransformer() = default;

  /**
   * Impl of transformation
   * @return
   */
  virtual shared_ptr<PhysicalPlan> transform();

  const shared_ptr<PrePhysicalPlan> &getPrePhysicalPlan() const;

protected:
  /**
   * Transform prephysical op to physical op in dfs style
   * @param prePOp: prephysical op
   * @return physical ops to be connect to its consumers
   */
  virtual vector<shared_ptr<PhysicalOp>> transformDfs(const shared_ptr<PrePhysicalOp> &prePOp);

  vector<vector<shared_ptr<PhysicalOp>>> transformProducers(const shared_ptr<PrePhysicalOp> &prePOp);

  vector<shared_ptr<PhysicalOp>> transformSort(const shared_ptr<SortPrePOp> &sortPrePOp);

  vector<shared_ptr<PhysicalOp>> transformLimitSort(const shared_ptr<LimitSortPrePOp> &limitSortPrePOp);

  vector<shared_ptr<PhysicalOp>> transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp);

  vector<shared_ptr<PhysicalOp>> transformGroup(const shared_ptr<GroupPrePOp> &groupPrePOp);

  vector<shared_ptr<PhysicalOp>> transformGroupOnePhase(const shared_ptr<GroupPrePOp> &groupPrePOp);

  vector<shared_ptr<PhysicalOp>> transformGroupTwoPhase(const shared_ptr<GroupPrePOp> &groupPrePOp);

  vector<shared_ptr<PhysicalOp>> transformProject(const shared_ptr<ProjectPrePOp> &projectPrePOp);

  vector<shared_ptr<PhysicalOp>> transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp);

  vector<shared_ptr<PhysicalOp>> transformHashJoin(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp);

  bool useBroadcastJoin(JoinType joinType, int64_t leftRowCount, int64_t rightRowCount);

  void transformHashJoinBcast(
          const std::shared_ptr<HashJoinPrePOp> &hashJoinPrePOp, const std::string &hashJoinPredicateStr,
          POpVec &upLeftConnPOps, POpVec &upRightConnPOps,
          POpVec &hashJoinArrowPOps, POpVec &allPOps,
          POpVec &ptPhaseOps, POpVec &joinPhaseOps, bool recordCard);

  void transformHashJoinPtion(
          const std::shared_ptr<HashJoinPrePOp> &hashJoinPrePOp, const std::string &hashJoinPredicateStr,
          POpVec &upLeftConnPOps, POpVec &upRightConnPOps,
          POpVec &hashJoinArrowPOps, POpVec &hashJoinBuildPOps, POpVec &hashJoinProbePOps,
          POpVec &allPOps, POpVec &ptPhaseOps, POpVec &joinPhaseOps, bool recordCard);

  vector<shared_ptr<PhysicalOp>> transformNestedLoopJoin(const shared_ptr<NestedLoopJoinPrePOp> &nestedLoopJoinPrePOp);

  vector<shared_ptr<PhysicalOp>> transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp);

  vector<shared_ptr<PhysicalOp>> transformUnionAll(const shared_ptr<UnionAllPrePOp> &unionAllPrePOp);

  vector<shared_ptr<PhysicalOp>> transformSeparableSuper(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp);

  void pushdownBloomFilter(const vector<shared_ptr<PhysicalOp>> &bloomFilterCreatePOps,
                           vector<shared_ptr<PhysicalOp>> &upRightConnPOps,
                           vector<shared_ptr<PhysicalOp>> &joinProbePOps,
                           const vector<string> &rightColumnNames,
                           bool withHashJoinPushdown);

  void pushdownBloomFilterNoHashJoinPushdown(const vector<shared_ptr<PhysicalOp>> &bloomFilterCreatePOps,
                                             vector<shared_ptr<PhysicalOp>> &upRightConnPOps,
                                             vector<shared_ptr<PhysicalOp>> &joinProbePOps,
                                             const vector<string> &rightColumnNames);

  void pushdownBloomFilterWithHashJoinPushdown(const vector<shared_ptr<PhysicalOp>> &bloomFilterCreatePOps,
                                               vector<shared_ptr<PhysicalOp>> &upRightConnPOps,
                                               const vector<string> &rightColumnNames);

  // return second stage of shuffles by node, which are upConnPOps
  vector<POpVec> batchExchangeShuffle(const POpVec &distPreShufflePOps,
                                      uint prePOpId,
                                      const std::string opNameBase,
                                      POpVec &allPOps,
                                      POpVec *joinPhasePOps /*if has value, then this shuffle is for join*/);

  void batchLoadShufflePushdown(const vector<shared_ptr<PhysicalOp>> &opsForShuffle,
                                const vector<shared_ptr<PhysicalOp>> &shuffleConsumers,
                                vector<shared_ptr<PhysicalOp>> &allPOps,
                                bool* isHashJoinArrowLeftConn = nullptr);

  void clear();

  /**
   * Func used by adaptive exec
   */
  void showAdaptSelectedDistJoinTypes() const;

  PrePToPTransformerType type_ = PrePToPTransformerType::REGULAR;
  shared_ptr<PrePhysicalPlan> prePhysicalPlan_;
  shared_ptr<CatalogueEntry> catalogueEntry_;
  shared_ptr<ObjStoreConnector> objStoreConnector_;
  shared_ptr<Mode> mode_;
  int parallelDegree_;
  int numNodes_;

  // state maintained during transformation
  unordered_map<string, shared_ptr<PhysicalOp>> physicalOps_;

  // used in adaptive execution
  long queryId_;
  bool isDistributed_;
  void* executor_;
  std::vector<std::pair<std::string, join::DistJoinType>> adaptDistJoinTypes_; // here we use a digest for ease of print

#if SHOW_DEBUG_METRICS == true
  // save transform results for some prephysical ops, used in predicate transfer metrics
  unordered_map<uint, vector<shared_ptr<PhysicalOp>>> prePOpIdToConnOpsForPredTrans_;
#endif
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMER_H
