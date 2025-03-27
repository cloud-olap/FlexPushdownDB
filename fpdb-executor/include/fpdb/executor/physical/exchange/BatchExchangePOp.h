//
// Created by Yifei Yang on 2/28/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_EXCHANGE_BATCHEXCHANGEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_EXCHANGE_BATCHEXCHANGEPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>

namespace fpdb::executor::physical::exchange {

/**
 * Collect and batch table data that should be transferred to other nodes,
 * the consumers can only be "ExchangeDstPOp"
 */
class BatchExchangePOp: public PhysicalOp {
  
public:
  BatchExchangePOp(const std::string &name,
                 const std::vector<std::string> &projectColumnNames,
                 int nodeId);
  BatchExchangePOp() = default;
  BatchExchangePOp(const BatchExchangePOp&) = default;
  BatchExchangePOp& operator=(const BatchExchangePOp&) = default;
  ~BatchExchangePOp() override = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void clear() override;

private:
  void onStart();
  void onTupleSetBuffer(const TupleSetBufferMessage &msg);
  void onComplete(const CompleteMessage &);

  void fetchConsumerNodeId();
  void send(int dstNodeId);

  // runtime states
  struct ExchangeBuffer{
    // received table to each node, but not yet wait for fetching
    // tables which are waiting for fetching are already moved to "FlightHandler::table_cache_"
    std::vector<std::shared_ptr<arrow::Table>> tables_;
  } buffer_;
  std::vector<std::string> dstNodeIdToConsumer_;
  std::unordered_map<std::string, int> consumerToDstNodeId_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BatchExchangePOp& op) {
    return inspect_base(f, op);
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_EXCHANGE_BATCHEXCHANGEPOP_H
