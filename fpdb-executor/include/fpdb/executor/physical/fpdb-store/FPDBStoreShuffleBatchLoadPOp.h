//
// Created by Yifei Yang on 11/29/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESHUFFLEBATCHLOADPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESHUFFLEBATCHLOADPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/TupleSetReadyRemoteMessage.h>

namespace fpdb::executor::physical::fpdb_store {

/**
 * Op to load shuffle result as a whole for the same compute node during pushdown, to avoid too many flight requests.
 */
class FPDBStoreShuffleBatchLoadPOp: public PhysicalOp {

public:
  FPDBStoreShuffleBatchLoadPOp(const std::string &name,
                               const std::vector<std::string> &projectColumnNames,
                               int nodeId);
  FPDBStoreShuffleBatchLoadPOp() = default;
  FPDBStoreShuffleBatchLoadPOp(const FPDBStoreShuffleBatchLoadPOp&) = default;
  FPDBStoreShuffleBatchLoadPOp& operator=(const FPDBStoreShuffleBatchLoadPOp&) = default;
  ~FPDBStoreShuffleBatchLoadPOp() = default;

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
  friend bool inspect(Inspector& f, FPDBStoreShuffleBatchLoadPOp& op) {
    return inspect_base(f, op);
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESHUFFLEBATCHLOADPOP_H
