//
// Created by Yifei Yang on 2/26/24.
//

#include <fpdb/executor/metrics/HashJoinMetrics.h>

namespace fpdb::executor::metrics {

HashJoinMetrics::HashJoinMetrics(int64_t numHtBuild, int64_t numHtProbe,
                                 int64_t numBfInsert, int64_t numBfFind):
  numHtBuild_(numHtBuild), numHtProbe_(numHtProbe),
  numBfInsert_(numBfInsert), numBfFind_(numBfFind) {}

HashJoinMetrics::HashJoinMetrics():
  numHtBuild_(0), numHtProbe_(0),
  numBfInsert_(0), numBfFind_(0) {}

int64_t HashJoinMetrics::getNumHtBuild() const {
  return numHtBuild_;
}

int64_t HashJoinMetrics::getNumHtProbe() const {
  return numHtProbe_;
}

int64_t HashJoinMetrics::getNumBfInsert() const {
  return numBfInsert_;
}

int64_t HashJoinMetrics::getNumBfFind() const {
  return numBfFind_;
}

void HashJoinMetrics::add(const HashJoinMetrics &other) {
  numHtBuild_ += other.numHtBuild_;
  numHtProbe_ += other.numHtProbe_;
  numBfInsert_ += other.numBfInsert_;
  numBfFind_ += other.numBfFind_;
}

}
