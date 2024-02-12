//
// Created by Yifei Yang on 11/29/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_SHUFFLEBATCHLOADPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_SHUFFLEBATCHLOADPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/TupleSetReadyRemoteMessage.h>

namespace fpdb::executor::physical::shuffle {

/**
 * Op to load shuffle result as a whole for the same compute node, to avoid too many flight requests.
 */
class ShuffleBatchLoadPOp: public PhysicalOp {

public:
  ShuffleBatchLoadPOp(const std::string &name,
                      const std::vector<std::string> &projectColumnNames,
                      int nodeId);
  ShuffleBatchLoadPOp() = default;
  ShuffleBatchLoadPOp(const ShuffleBatchLoadPOp&) = default;
  ShuffleBatchLoadPOp& operator=(const ShuffleBatchLoadPOp&) = default;
  ~ShuffleBatchLoadPOp() = default;

  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTupleSetReadyRemote(const TupleSetReadyRemoteMessage &msg);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ShuffleBatchLoadPOp& op) {
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


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_SHUFFLEBATCHLOADPOP_H
