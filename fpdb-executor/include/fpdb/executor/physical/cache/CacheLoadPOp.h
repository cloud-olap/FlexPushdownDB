//
// Created by matt on 8/7/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_CACHE_CACHELOADPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_CACHE_CACHELOADPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/Envelope.h>
#include <fpdb/executor/message/cache/LoadResponseMessage.h>
#include <fpdb/catalogue/Partition.h>
#include <fpdb/catalogue/obj-store/ObjStoreConnector.h>

using namespace fpdb::executor::physical;
using namespace fpdb::executor::message;
using namespace fpdb::catalogue::obj_store;

namespace fpdb::executor::physical::cache {

class CacheLoadPOp : public PhysicalOp {

public:
  explicit CacheLoadPOp(const std::string &name,
                        const std::vector<std::string> &projectColumnNames,
                        int nodeId,
                        const std::vector<std::string> &predicateColumnNames,
                        const std::vector<std::set<std::string>> &projectColumnGroups,
                        const std::vector<std::string> &allColumnNames,
                        const std::shared_ptr<Partition> &partition,
                        int64_t startOffset,
                        int64_t finishOffset,
                        const std::optional<std::shared_ptr<ObjStoreConnector>> &objStoreConnector);
  CacheLoadPOp() = default;
  CacheLoadPOp(const CacheLoadPOp&) = default;
  CacheLoadPOp& operator=(const CacheLoadPOp&) = default;
  ~CacheLoadPOp() override = default;

  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;

  const std::shared_ptr<Partition> &getPartition() const;

  void setHitOperator(const std::shared_ptr<PhysicalOp> &op);
  void setMissOperatorToCache(const std::shared_ptr<PhysicalOp> &op);
  void setMissOperatorToPushdown(const std::shared_ptr<PhysicalOp> &op);
  void enableBitmapPushdown();

private:
  void requestLoadSegmentsFromCache();
  void onStart();
  void onCacheLoadResponse(const LoadResponseMessage &message);

  /**
   * allColumnNames = projectColumnNames + predicateColumnNames
   * projectColumnGroups are used only for hybrid execution, e.g. for aggregate operator with sum(A * B) and sum(C * D),
   * (A, B) and (C, D) are two groups, which are processed separately, e.g. one is executed using cache,
   * the other by pushdown
   */
  std::vector<std::string> predicateColumnNames_;
  std::vector<std::set<std::string>> projectColumnGroups_;
  std::vector<std::string> allColumnNames_;

  std::shared_ptr<Partition> partition_;
  int64_t startOffset_;
  int64_t finishOffset_;

  std::optional<std::string> hitOperatorName_;
  std::optional<std::string> missOperatorToCacheName_;
  std::optional<std::string> missOperatorToPushdownName_;

  std::optional<std::shared_ptr<ObjStoreConnector>> objStoreConnector_;

  /**
   * Used for bitmap pushdown, i.e. decide whether to send predicateColumnNames to missOperatorToPushdown
   */
  bool isBitmapPushdownEnabled_ = false;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CacheLoadPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("consumerToBloomFilterInfo", op.consumerToBloomFilterInfo_),
                               f.field("isSeparated", op.isSeparated_),
                               f.field("predicateColumnNames", op.predicateColumnNames_),
                               f.field("projectColumnGroups", op.projectColumnGroups_),
                               f.field("allColumnNames", op.allColumnNames_),
                               f.field("partition", op.partition_),
                               f.field("startOffset", op.startOffset_),
                               f.field("finishOffset", op.finishOffset_),
                               f.field("hitOperatorName", op.hitOperatorName_),
                               f.field("missOperatorToCacheName", op.missOperatorToCacheName_),
                               f.field("missOperatorToPushdownName", op.missOperatorToPushdownName_),
                               f.field("objStoreConnector", op.objStoreConnector_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_CACHE_CACHELOADPOP_H
