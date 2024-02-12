//
// Created by Yifei Yang on 3/7/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DISKMETRICS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DISKMETRICS_H

#include <fpdb/executor/metrics/Globals.h>

namespace fpdb::executor::metrics {

class DiskMetrics {
  
public:
  DiskMetrics(int64_t bytesFromDisk);
  DiskMetrics();
  DiskMetrics(const DiskMetrics&) = default;
  DiskMetrics& operator=(const DiskMetrics&) = default;
  ~DiskMetrics() = default;

  int64_t getBytesFromDisk() const;
  void add(const DiskMetrics &other);

private:
  int64_t bytesFromDisk_;

  // caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DiskMetrics& metrics) {
    return f.object(metrics).fields(f.field("bytesFromDisk", metrics.bytesFromDisk_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DISKMETRICS_H
