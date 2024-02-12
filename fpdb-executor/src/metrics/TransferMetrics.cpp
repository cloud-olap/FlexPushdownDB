//
// Created by Yifei Yang on 12/15/22.
//

#include <fpdb/executor/metrics/TransferMetrics.h>

namespace fpdb::executor::metrics {

TransferMetrics::TransferMetrics(int64_t bytesFromStore,
                                 int64_t bytesToStore,
                                 int64_t bytesInterCompute):
  bytesFromStore_(bytesFromStore),
  bytesToStore_(bytesToStore),
  bytesInterCompute_(bytesInterCompute) {}

TransferMetrics::TransferMetrics():
  bytesFromStore_(0),
  bytesToStore_(0),
  bytesInterCompute_(0) {}

int64_t TransferMetrics::getBytesFromStore() const {
  return bytesFromStore_;
}

int64_t TransferMetrics::getBytesToStore() const {
  return bytesToStore_;
}

int64_t TransferMetrics::getBytesInterCompute() const {
  return bytesInterCompute_;
}

void TransferMetrics::add(const TransferMetrics &other) {
  bytesFromStore_ += other.bytesFromStore_;
  bytesToStore_ += other.bytesToStore_;
  bytesInterCompute_ += other.bytesInterCompute_;
}
  
}
