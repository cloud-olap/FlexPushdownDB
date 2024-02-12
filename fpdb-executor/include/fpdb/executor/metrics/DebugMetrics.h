//
// Created by Yifei Yang on 3/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DEBUGMETRICS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DEBUGMETRICS_H

#include <fpdb/executor/metrics/TransferMetrics.h>
#include <fpdb/executor/metrics/DiskMetrics.h>
#include <fpdb/executor/metrics/PredTransMetrics.h>
#include <mutex>

namespace fpdb::executor::metrics {

class DebugMetrics {

public:
  DebugMetrics() = default;

  const TransferMetrics &getTransferMetrics() const;
  const DiskMetrics &getDiskMetrics() const;
  const PredTransMetrics &getPredTransMetrics() const;
  int getNumPushdownFallBack() const;

  void add(const TransferMetrics &transferMetrics);
  void add(const DiskMetrics &diskMetrics);
  void add(const PredTransMetrics::PTMetricsUnit &ptMetricsUnit);
  void incPushdownFallBack();

private:
  TransferMetrics transferMetrics_;
  DiskMetrics diskMetrics_;
  PredTransMetrics ptMetrics_;
  int numPushdownFallBack_ = 0;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DEBUGMETRICS_H
