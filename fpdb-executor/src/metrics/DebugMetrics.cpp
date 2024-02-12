//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/metrics/DebugMetrics.h>

namespace fpdb::executor::metrics {

const TransferMetrics &DebugMetrics::getTransferMetrics() const {
  return transferMetrics_;
}

const DiskMetrics &DebugMetrics::getDiskMetrics() const {
  return diskMetrics_;
}

const PredTransMetrics &DebugMetrics::getPredTransMetrics() const {
  return ptMetrics_;
}

int DebugMetrics::getNumPushdownFallBack() const {
  return numPushdownFallBack_;
}

void DebugMetrics::add(const TransferMetrics &transferMetrics) {
  transferMetrics_.add(transferMetrics);
}

void DebugMetrics::add(const DiskMetrics &diskMetrics) {
  diskMetrics_.add(diskMetrics);
}

void DebugMetrics::add(const PredTransMetrics::PTMetricsUnit &ptMetricsUnit) {
  ptMetrics_.add(ptMetricsUnit);
}

void DebugMetrics::incPushdownFallBack() {
  ++numPushdownFallBack_;
}

}
