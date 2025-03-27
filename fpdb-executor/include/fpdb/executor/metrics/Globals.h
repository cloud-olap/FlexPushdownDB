//
// Created by Yifei Yang on 3/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_GLOBALS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_GLOBALS_H

#define SHOW_DEBUG_METRICS true

#include <memory>

namespace fpdb::executor::metrics {

// regular metrics
inline bool SHOW_OP_TIME = false;
inline bool SHOW_OP_TYPE_TIME = true;
inline bool SHOW_SCAN_METRICS = false;

// debug metrics
inline bool SHOW_NETWORK_METRICS = true;
inline bool SHOW_DISK_METRICS = false;     // FIXME: currently this is only visible at the storage side
inline bool SHOW_PRED_TRANS_METRICS = false;
inline bool SHOW_PRED_TRANS_CS_METRICS = false;   // Metrics for `case study` in the paper
inline bool SHOW_HASH_JOIN_METRICS = false;
inline bool SHOW_BLOOM_FILTER_METRICS = false;
inline bool SHOW_NUM_PUSHDOWN_FALL_BACK = false;

inline bool hasRegularMetricsToShow() {
  return SHOW_OP_TIME || SHOW_OP_TYPE_TIME || SHOW_SCAN_METRICS;
}

inline bool hasDebugMetricsToShow() {
  return SHOW_NETWORK_METRICS || SHOW_DISK_METRICS || SHOW_PRED_TRANS_METRICS || SHOW_HASH_JOIN_METRICS ||
      SHOW_BLOOM_FILTER_METRICS || SHOW_NUM_PUSHDOWN_FALL_BACK;
}

// optimizer metrics
inline bool SHOW_DIST_PRED_TRANS_TYPE = true;
inline bool SHOW_DIST_JOIN_TYPE = true;

// progress bar
inline constexpr bool SHOW_PROGRESS_BAR = true;

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_GLOBALS_H
