//
// Created by Yifei Yang on 2/26/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_HASHJOINMETRICS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_HASHJOINMETRICS_H

#include <fpdb/executor/metrics/Globals.h>

namespace fpdb::executor::metrics {

class HashJoinMetrics {
  
public:
  HashJoinMetrics(int64_t numHtBuild, int64_t numHtProbe,
                  int64_t numBfInsert, int64_t numBfFind);
  HashJoinMetrics();
  HashJoinMetrics(const HashJoinMetrics&) = default;
  HashJoinMetrics& operator=(const HashJoinMetrics&) = default;
  ~HashJoinMetrics() = default;

  int64_t getNumHtBuild() const;
  int64_t getNumHtProbe() const;
  int64_t getNumBfInsert() const;
  int64_t getNumBfFind() const;
  void add(const HashJoinMetrics &other);

private:
  int64_t numHtBuild_;
  int64_t numHtProbe_;
  int64_t numBfInsert_;
  int64_t numBfFind_;

  // caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinMetrics& metrics) {
    return f.object(metrics).fields(f.field("numHtBuild", metrics.numHtBuild_),
                                    f.field("numHtProbe", metrics.numHtProbe_),
                                    f.field("numBfInsert", metrics.numBfInsert_),
                                    f.field("numBfFind", metrics.numBfFind_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_HASHJOINMETRICS_H
