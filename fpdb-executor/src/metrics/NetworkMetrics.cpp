//
// Created by Yifei Yang on 12/15/22.
//

#include <fpdb/executor/metrics/NetworkMetrics.h>

namespace fpdb::executor::metrics {

NetworkMetrics::NetworkMetrics(int64_t bytesFromStore,
                               int64_t bytesToStore,
                               int64_t bytesInterCompute):
  bytesFromStore_(bytesFromStore),
  bytesToStore_(bytesToStore),
  bytesInterCompute_(bytesInterCompute) {}

NetworkMetrics::NetworkMetrics():
  bytesFromStore_(0),
  bytesToStore_(0),
  bytesInterCompute_(0) {}

int64_t NetworkMetrics::getBytesFromStore() const {
  return bytesFromStore_;
}

int64_t NetworkMetrics::getBytesToStore() const {
  return bytesToStore_;
}

int64_t NetworkMetrics::getBytesInterCompute() const {
  return bytesInterCompute_;
}

void NetworkMetrics::add(const NetworkMetrics &other) {
  bytesFromStore_ += other.bytesFromStore_;
  bytesToStore_ += other.bytesToStore_;
  bytesInterCompute_ += other.bytesInterCompute_;
}
  
}
