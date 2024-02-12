//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/message/TransferMetricsMessage.h>

namespace fpdb::executor::message {

TransferMetricsMessage::TransferMetricsMessage(const executor::metrics::TransferMetrics &transferMetrics,
                                               const std::string &sender):
  Message(TRANSFER_METRICS, sender),
  transferMetrics_(transferMetrics) {}

std::string TransferMetricsMessage::getTypeString() const {
  return "TransferMetricsMessage";
}

const executor::metrics::TransferMetrics &TransferMetricsMessage::getTransferMetrics() const {
  return transferMetrics_;
}

}
