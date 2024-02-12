//
// Created by Yifei Yang on 11/21/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOS3PTRANSFORMER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOS3PTRANSFORMER_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/plan/Mode.h>
#include <fpdb/catalogue/Partition.h>
#include <fpdb/catalogue/obj-store/s3/S3Connector.h>

using namespace fpdb::plan;
using namespace fpdb::plan::prephysical;
using namespace fpdb::plan::prephysical::separable;
using namespace fpdb::expression::gandiva;
using namespace fpdb::catalogue;
using namespace fpdb::catalogue::obj_store;

namespace fpdb::executor::physical {

class PrePToS3PTransformer {

public:
  /**
   * Transform separable super prephysical op to physical op
   * @return a pair of connect physical ops (to consumers) and current all (cumulative) physical ops
   */
  static pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  transform(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp,
            const shared_ptr<Mode> &mode,
            int numNodes,
            const shared_ptr<S3Connector> &s3Connector);

  /**
   * Add BloomFilterUsePOp to S3SelectPOp
   * if producers are S3SelectPOp and bloom filter pushdown is enabled
   * @param producers
   * @param bloomFilterUsePOps
   * @return a pair of connect physical ops (to consumers) and additional physical ops to add to plan
   */
  static pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>>
  addBloomFilterUse(vector<shared_ptr<PhysicalOp>> &producers,
                    vector<shared_ptr<PhysicalOp>> &bloomFilterUsePOps,
                    const shared_ptr<Mode> &mode);

private:
  PrePToS3PTransformer(const shared_ptr<SeparableSuperPrePOp> &separableSuperPrePOp,
                       const shared_ptr<Mode> &mode,
                       int numNodes,
                       const shared_ptr<S3Connector> &s3Connector);

  /**
   * Impl of transformation
   * @return a pair of connect physical ops (to consumers) and current all (cumulative) physical ops
   */
  pair<vector<shared_ptr<PhysicalOp>>, vector<shared_ptr<PhysicalOp>>> transform();

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
  transformFilterableScanHybrid(const shared_ptr<FilterableScanPrePOp> &filterableScanPrePOp,
                                const unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate> &partitionPredicates);

  /**
   * Generate s3 select where clause from filter predicate
   * @param predicate
   * @return
   */
  string genFilterSql(const std::shared_ptr<Expression>& predicate);

  shared_ptr<SeparableSuperPrePOp> separableSuperPrePOp_;
  shared_ptr<Mode> mode_;
  int numNodes_;
  shared_ptr<S3Connector> s3Connector_;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOS3PTRANSFORMER_H
