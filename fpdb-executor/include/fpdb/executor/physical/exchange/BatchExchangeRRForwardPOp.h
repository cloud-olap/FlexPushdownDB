//
// Created by Yifei Yang on 2/29/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_EXCHANGE_BATCHEXCHANGERRFORWARDPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_EXCHANGE_BATCHEXCHANGERRFORWARDPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>

namespace fpdb::executor::physical::exchange {

/**
 * Used when "ENABLE_PARALLEL_BATCH_EXCHANGE = true", where batches transferred to a node are received by multiple
 * receivers in parallel, i.e., this op forwards batches to its consumers to fetch in parallel,
 * in a round-robin fashion
 */
class BatchExchangeRRForwardPOp: public PhysicalOp {

public:
  BatchExchangeRRForwardPOp(const std::string &name,
                            const std::vector<std::string> &projectColumnNames,
                            int nodeId);
  BatchExchangeRRForwardPOp() = default;
  BatchExchangeRRForwardPOp(const BatchExchangeRRForwardPOp&) = default;
  BatchExchangeRRForwardPOp& operator=(const BatchExchangeRRForwardPOp&) = default;
  ~BatchExchangeRRForwardPOp() override = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void produce(const std::shared_ptr<PhysicalOp> &op) override;
  void clear() override;

private:
  void onStart();
  void onTupleSet(const TupleSetMessage &msg);
  void onTupleSetReadyRemote(const TupleSetReadyRemoteMessage &msg); // this overrides the default behavior in "POpActor"
  void onComplete(const CompleteMessage &);

  std::vector<std::string> consumerVec_;
  int nextConsumerId_ = 0;    // the index of consumer in "consumerVec_" to fetch the next batch

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BatchExchangeRRForwardPOp& op) {
    return inspect_base(f, op,
                        f.field("consumerVec", op.consumerVec_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_EXCHANGE_BATCHEXCHANGERRFORWARDPOP_H
