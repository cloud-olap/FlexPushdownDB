//
// Created by Yifei Yang on 3/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TRANSFERMETRICSMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TRANSFERMETRICSMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/metrics/TransferMetrics.h>
#include <memory>

namespace fpdb::executor::message {

class TransferMetricsMessage : public Message {

public:
  TransferMetricsMessage(const executor::metrics::TransferMetrics &transferMetrics,
                         const std::string &sender);
  TransferMetricsMessage() = default;
  TransferMetricsMessage(const TransferMetricsMessage&) = default;
  TransferMetricsMessage& operator=(const TransferMetricsMessage&) = default;
  ~TransferMetricsMessage() override = default;

  std::string getTypeString() const override;

  const executor::metrics::TransferMetrics &getTransferMetrics() const;

private:
  executor::metrics::TransferMetrics transferMetrics_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TransferMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("transferMetrics", msg.transferMetrics_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TRANSFERMETRICSMESSAGE_H
