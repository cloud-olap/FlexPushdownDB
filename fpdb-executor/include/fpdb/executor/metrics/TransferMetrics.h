//
// Created by Yifei Yang on 12/15/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_TRANSFERMETRICS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_TRANSFERMETRICS_H

#include <fpdb/executor/metrics/Globals.h>

namespace fpdb::executor::metrics {

class TransferMetrics {

public:
  TransferMetrics(int64_t bytesFromStore,
                  int64_t bytesToStore,
                  int64_t bytesInterCompute);
  TransferMetrics();
  TransferMetrics(const TransferMetrics&) = default;
  TransferMetrics& operator=(const TransferMetrics&) = default;
  ~TransferMetrics() = default;

  int64_t getBytesFromStore() const;
  int64_t getBytesToStore() const;
  int64_t getBytesInterCompute() const;
  void add(const TransferMetrics &other);

private:
  int64_t bytesFromStore_;
  int64_t bytesToStore_;
  int64_t bytesInterCompute_;

  // caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TransferMetrics& metrics) {
    return f.object(metrics).fields(f.field("bytesFromStore", metrics.bytesFromStore_),
                                    f.field("bytesToStore", metrics.bytesToStore_),
                                    f.field("bytesInterCompute", metrics.bytesInterCompute_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_TRANSFERMETRICS_H
