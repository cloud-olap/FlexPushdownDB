//
// Created by Yifei Yang on 10/28/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BROADCAST_BROADCASTPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BROADCAST_BROADCASTPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/BloomFilterMessage.h>

namespace fpdb::executor::physical::broadcast {

/**
 * Broadcast data (e.g. tupleSet, bloom filter) to all consumers.
 */
class BroadcastPOp: public PhysicalOp {
public:
  BroadcastPOp(const std::string &name,
               const std::vector<std::string> &projectColumnNames,
               int nodeId,
               bool enableBatchExchange = false);
  BroadcastPOp() = default;
  BroadcastPOp(const BroadcastPOp&) = default;
  BroadcastPOp& operator=(const BroadcastPOp&) = default;
  ~BroadcastPOp() override = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void clear() override;
  void consume(const std::shared_ptr<PhysicalOp> &op) override;

  const std::vector<std::string> &getOrderedProducers() const;

private:
  void onStart();
  void onBloomFilter(const BloomFilterMessage &msg);
  void onTupleSet(const TupleSetMessage &msg);
  void onComplete(const CompleteMessage &);

  std::vector<std::string> orderedProducers_;   // maintain producers with incoming order, used in hashjoin pushdown

  // for batch exchange
  bool enableBatchExchange_ = false;
  std::shared_ptr<TupleSet> batchExchangeBuffer_ = nullptr;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BroadcastPOp& op) {
    return inspect_base(f, op,
                        f.field("orderedProducers", op.orderedProducers_),
                        f.field("enableBatchExchange", op.enableBatchExchange_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BROADCAST_BROADCASTPOP_H