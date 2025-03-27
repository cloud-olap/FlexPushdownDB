//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/message/NetworkMetricsMessage.h>

namespace fpdb::executor::message {

NetworkMetricsMessage::NetworkMetricsMessage(const executor::metrics::NetworkMetrics &NetworkMetrics,
                                             const std::string &sender):
  Message(NETWORK_METRICS, sender),
  NetworkMetrics_(NetworkMetrics) {}

std::string NetworkMetricsMessage::getTypeString() const {
  return "NetworkMetricsMessage";
}

const executor::metrics::NetworkMetrics &NetworkMetricsMessage::getNetworkMetrics() const {
  return NetworkMetrics_;
}

}
