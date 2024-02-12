//
// Created by Yifei Yang on 3/7/23.
//

#include <fpdb/executor/metrics/DiskMetrics.h>

namespace fpdb::executor::metrics {

DiskMetrics::DiskMetrics(int64_t bytesFromDisk):
  bytesFromDisk_(bytesFromDisk) {}

DiskMetrics::DiskMetrics():
  bytesFromDisk_(0) {}

int64_t DiskMetrics::getBytesFromDisk() const {
  return bytesFromDisk_;
}

void DiskMetrics::add(const DiskMetrics &other) {
  bytesFromDisk_ += other.bytesFromDisk_;
}

}
