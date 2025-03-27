//
// Created by Yifei Yang on 3/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_NETWORKMETRICSMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_NETWORKMETRICSMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/metrics/NetworkMetrics.h>
#include <memory>

namespace fpdb::executor::message {

class NetworkMetricsMessage : public Message {

public:
  NetworkMetricsMessage(const executor::metrics::NetworkMetrics &NetworkMetrics,
                        const std::string &sender);
  NetworkMetricsMessage() = default;
  NetworkMetricsMessage(const NetworkMetricsMessage&) = default;
  NetworkMetricsMessage& operator=(const NetworkMetricsMessage&) = default;
  ~NetworkMetricsMessage() override = default;

  std::string getTypeString() const override;

  const executor::metrics::NetworkMetrics &getNetworkMetrics() const;

private:
  executor::metrics::NetworkMetrics NetworkMetrics_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, NetworkMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("NetworkMetrics", msg.NetworkMetrics_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_NETWORKMETRICSMESSAGE_H
