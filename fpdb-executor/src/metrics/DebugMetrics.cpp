//
// Created by Yifei Yang on 3/14/22.
//

#include <fpdb/executor/metrics/DebugMetrics.h>

namespace fpdb::executor::metrics {

const NetworkMetrics &DebugMetrics::getNetworkMetrics() const {
  return NetworkMetrics_;
}

const DiskMetrics &DebugMetrics::getDiskMetrics() const {
  return diskMetrics_;
}

const PredTransMetrics &DebugMetrics::getPredTransMetrics() const {
  return ptMetrics_;
}

const PredTransCSMetrics &DebugMetrics::getPredTransCSMetrics() const {
  return ptCSMetrics_;
}

const HashJoinMetrics &DebugMetrics::getHashJoinMetrics() const {
  return hjMetrics_;
}

int DebugMetrics::getNumPushdownFallBack() const {
  return numPushdownFallBack_;
}

void DebugMetrics::add(const NetworkMetrics &NetworkMetrics) {
  NetworkMetrics_.add(NetworkMetrics);
}

void DebugMetrics::add(const DiskMetrics &diskMetrics) {
  diskMetrics_.add(diskMetrics);
}

void DebugMetrics::add(const PredTransMetrics::PTMetricsUnit &ptMetricsUnit) {
  ptMetrics_.add(ptMetricsUnit);
}

void DebugMetrics::add(const PredTransCSMetrics::PTCSMetricsUnit &ptCSMetricsUnit) {
  ptCSMetrics_.add(ptCSMetricsUnit);
}

void DebugMetrics::add(const HashJoinMetrics &hjMetrics) {
  hjMetrics_.add(hjMetrics);
}

void DebugMetrics::incPushdownFallBack() {
  ++numPushdownFallBack_;
}

}
