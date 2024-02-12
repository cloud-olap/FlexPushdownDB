//
// Created by Yifei Yang on 3/3/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOFPDBSTOREPTRANSFORMER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOFPDBSTOREPTRANSFORMER_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFileScanPOp.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/FilterPrePOp.h>
#include <fpdb/plan/prephysical/ProjectPrePOp.h>
#include <fpdb/plan/prephysical/AggregatePrePOp.h>
#include <fpdb/plan/prephysical/HashJoinPrePOp.h>
#include <fpdb/plan/Mode.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>
#include <fpdb/expression/gandiva/Expression.h>

using namespace fpdb::plan;
using namespace fpdb::plan::prephysical;
using namespace fpdb::plan::prephysical::separable;
using namespace fpdb::catalogue::obj_store;
using namespace fpdb::expression::gandiva;

namespace fpdb::executor::physical {

class PrePToFPDBStorePTransformer {

public:
  /**
   * Transform separable super prephysical op to physical op
   * @return a pair of connect physical ops (to consumers) and current all (cumulative) physical ops
   */
  static pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transform(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp,
            const shared_ptr<Mode> &mode,
            int numNodes,
            int computeParallelDegree,
            int fpdbStoreParallelDegree,
            const shared_ptr<FPDBStoreConnector> &fpdbStoreConnector);

  /**
   * Add each of the separable operators to the corresponding FPDBStoreSuperPOp
   * if the producers is FPDBStoreSuperPOp and the operator type is enabled for pushdown
   * @param producers
   * @param separablePOps
   * @param opMap
   * @param mode
   * @return a tuple of "connect physical ops (to consumers)", "additional physical ops to add to plan", and
   * "whether hash join is pushed"
   */
  static std::tuple<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>, bool>
  addSeparablePOp(vector<shared_ptr<PhysicalOp>> &producers,
                  vector<shared_ptr<PhysicalOp>> &separablePOps,
                  const shared_ptr<Mode> &mode,
                  unordered_map<string, shared_ptr<PhysicalOp>>* opMap,
                  int prePOpId = 0,
                  int numComputeNodes = 0,
                  int computeParallelDegree = 0);

  /**
   * The case when there is no hash-join pushdown, which is the regular case
   */
  static pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  addSeparablePOpNoHashJoinPushdown(vector<shared_ptr<PhysicalOp>> &producers,
                                    vector<shared_ptr<PhysicalOp>> &separablePOps);

  /**
   * The case when there is hash-join pushdown
   * "producers" should all be "FPDBStoreTableCacheLoadPOp"
   * "separablePOps" should be the same type
   */
  static pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  addSeparablePOpWithHashJoinPushdown(vector<shared_ptr<PhysicalOp>> &producers,
                                      vector<shared_ptr<PhysicalOp>> &separablePOps,
                                      unordered_map<string, shared_ptr<PhysicalOp>>* opMap,
                                      int prePOpId,
                                      int numComputeNodes,
                                      int computeParallelDegree);

private:
  PrePToFPDBStorePTransformer(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp,
                              const shared_ptr<Mode> &mode,
                              int numNodes,
                              int computeParallelDegree,
                              int fpdbStoreParallelDegree,
                              const shared_ptr<FPDBStoreConnector> &fpdbStoreConnector);
  
  /**
   * Impl of transformation
   * @return 
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>> transform();
  
  /**
   * Transform prephysical op to physical op in dfs style
   * @param prePOp: prephysical op
   * @return a pair of connect physical ops (to consumers) and current all (cumulative) physical ops
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformDfs(const shared_ptr<PrePhysicalOp> &prePOp);

  vector<pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>>
  transformProducers(const shared_ptr<PrePhysicalOp> &prePOp);

  /**
   * Transform the plan of pushdown-only into hybrid execution, where the plan of pushdown-only is just FpdbStoreSuperPOps
   * @param fpdbStoreSuperPOps
   * @return a pair of connect physical ops (to consumers) and current all (cumulative) physical ops
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformPushdownOnlyToHybrid(const vector<shared_ptr<PhysicalOp>> &fpdbStoreSuperPOps);

  void enableBitmapPushdown(const unordered_map<shared_ptr<PhysicalOp>, shared_ptr<PhysicalOp>> &storePOpToLocalPOp,
                            const shared_ptr<fpdb_store::FPDBStoreSuperPOp> &fpdbStoreSuperPOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScan(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanPullup(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanPushdownOnly(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                      const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilterableScanCachingOnly(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                     const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformProject(const shared_ptr<ProjectPrePOp> &projectPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformFilter(const shared_ptr<FilterPrePOp> &filterPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformAggregate(const shared_ptr<AggregatePrePOp> &aggregatePrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformHashJoin(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformHashJoinNoPushdown(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp);

  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transformHashJoinPushdown(const shared_ptr<HashJoinPrePOp> &hashJoinPrePOp);

  shared_ptr<SeparableSuperPrePOp> separableSuperPrePOp_;
  std::shared_ptr<Mode> mode_;
  int numComputeNodes_;
  size_t numFPDBStoreNodes_;
  int computeParallelDegree_;
  int fpdbStoreParallelDegree_;
  std::shared_ptr<FPDBStoreConnector> fpdbStoreConnector_;

  // temp variables
  unordered_map<std::string, std::string> objectToHost_;
  struct HashJoinTransInfo {
    bool enabled_ = false;
    unordered_map<std::string, int> opToStoreNode_;
  } hashJoinTransInfo_;   // only used when pushing hash joins

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOFPDBSTOREPTRANSFORMER_H
