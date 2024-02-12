//
// Created by Yifei Yang on 10/6/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORETABLECACHELOADPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORETABLECACHELOADPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>

namespace fpdb::executor::physical::fpdb_store {

/**
 * Op to consume the table generated at storage side.
 * This is used only when pushing a big query plan (e.g. hash join) to store, to ensure pipelining (i.e. wait at
 * storage side and get table back immediately when generated).
 */
class FPDBStoreTableCacheLoadPOp: public PhysicalOp{
  
public:
  FPDBStoreTableCacheLoadPOp(const std::string &name,
                             const std::vector<std::string> &projectColumnNames,
                             int nodeId);
  FPDBStoreTableCacheLoadPOp() = default;
  FPDBStoreTableCacheLoadPOp(const FPDBStoreTableCacheLoadPOp&) = default;
  FPDBStoreTableCacheLoadPOp& operator=(const FPDBStoreTableCacheLoadPOp&) = default;
  ~FPDBStoreTableCacheLoadPOp() = default;

  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;
  void consume(const std::shared_ptr<PhysicalOp> &op) override;

  const std::string &getProducer() const;

private:
  void onStart();
  void onTupleSetWaitRemote(const TupleSetWaitRemoteMessage &msg);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FPDBStoreTableCacheLoadPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("consumerToBloomFilterInfo", op.consumerToBloomFilterInfo_),
                               f.field("isSeparated", op.isSeparated_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORETABLECACHELOADPOP_H
