//
// Created by Yifei Yang on 3/7/23.
//

#include <fpdb/executor/message/DiskMetricsMessage.h>

namespace fpdb::executor::message {

DiskMetricsMessage::DiskMetricsMessage(const executor::metrics::DiskMetrics &diskMetrics,
                                       const std::string &sender):
  Message(DISK_METRICS, sender),
  diskMetrics_(diskMetrics) {}

std::string DiskMetricsMessage::getTypeString() const {
  return "DiskMetricsMessage";
}

const executor::metrics::DiskMetrics &DiskMetricsMessage::getDiskMetrics() const {
  return diskMetrics_;
}

}
