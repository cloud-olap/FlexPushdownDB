//
// Created by Yifei Yang on 3/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_GLOBALS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_GLOBALS_H

#define SHOW_DEBUG_METRICS true

#include <memory>

namespace fpdb::executor::metrics {

/**
 * The followings only make effects when SHOW_DEBUG_METRICS is true
 */
inline bool SHOW_TRANSFER_METRICS = true;
inline bool SHOW_DISK_METRICS = false;     // FIXME: currently this is only visible at the storage side
inline bool SHOW_PRED_TRANS_METRICS = true;
inline bool SHOW_HASH_JOIN_METRICS = true;
inline bool SHOW_BLOOM_FILTER_METRICS = true;
inline bool SHOW_NUM_PUSHDOWN_FALL_BACK = false;

inline bool hasMetricsToShow() {
  return SHOW_TRANSFER_METRICS || SHOW_DISK_METRICS || SHOW_PRED_TRANS_METRICS || SHOW_HASH_JOIN_METRICS ||
      SHOW_BLOOM_FILTER_METRICS || SHOW_NUM_PUSHDOWN_FALL_BACK;
}

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_GLOBALS_H
