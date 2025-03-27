//
// Created by Yifei Yang on 3/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DEBUGMETRICS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DEBUGMETRICS_H

#include <fpdb/executor/metrics/NetworkMetrics.h>
#include <fpdb/executor/metrics/DiskMetrics.h>
#include <fpdb/executor/metrics/PredTransMetrics.h>
#include <fpdb/executor/metrics/HashJoinMetrics.h>
#include <mutex>

namespace fpdb::executor::metrics {

class DebugMetrics {

public:
  DebugMetrics() = default;

  const NetworkMetrics &getNetworkMetrics() const;
  const DiskMetrics &getDiskMetrics() const;
  const PredTransMetrics &getPredTransMetrics() const;
  const PredTransCSMetrics &getPredTransCSMetrics() const;
  const HashJoinMetrics &getHashJoinMetrics() const;
  int getNumPushdownFallBack() const;

  void add(const NetworkMetrics &NetworkMetrics);
  void add(const DiskMetrics &diskMetrics);
  void add(const PredTransMetrics::PTMetricsUnit &ptMetricsUnit);
  void add(const PredTransCSMetrics::PTCSMetricsUnit &ptCSMetricsUnit);
  void add(const HashJoinMetrics &hjMetrics);
  void incPushdownFallBack();

private:
  NetworkMetrics NetworkMetrics_;
  DiskMetrics diskMetrics_;
  PredTransMetrics ptMetrics_;
  PredTransCSMetrics ptCSMetrics_;
  HashJoinMetrics hjMetrics_;
  int numPushdownFallBack_ = 0;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_DEBUGMETRICS_H
