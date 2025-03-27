//
// Created by Yifei Yang on 11/17/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GLOBALS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GLOBALS_H

#include <fpdb/executor/physical/transform/pred-trans/PredTransOrder.h>
#include <fpdb/executor/physical/transform/pred-trans/DistPredTransType.h>
#include <fpdb/executor/physical/join/DistJoinType.h>
#include <stdint.h>
#include <string>

namespace fpdb::executor::physical {

/**
 * Default number of tuples that operators should buffer before sending to consumers.
 * Default number of bytes for S3 conversion.
 * Default number of bytes when doing a S3 range scan.
 */
inline constexpr int DefaultBufferSize = 100000;
inline constexpr int DefaultS3ConversionBufferSize = 128 * 1024;
// FIXME: temporary fix of "parseChunkSize < payload size" issue on Airmettle Select
inline constexpr int DefaultS3ConversionBufferSizeAirmettleSelect = 16 * 1024 * 1024;
inline constexpr uint64_t DefaultS3RangeSize = 15 * 1024 * 1024; // 15MB/s This value was tuned on c5n.9xlarge and
// may need to be retuned for different instances with many more cores

/**
 * Parameters used in WLFU, with csv_150MB/ and 200 parallel reqs
 */
// c5a.8x
inline constexpr double vNetwork = 1.16320;     // unit: GB/s
inline constexpr double vS3Scan = 18.00891;     // unit: GB/s
inline constexpr double vS3Filter = 0.32719;    // unit: GPred/s

/**
 * These parameters are for running GET in parallel as a detached operation.
 * We only want to convert ~max cores results at a time since otherwise we get very bad cache thrashing
 * that degrades system performance. Additionally setting a variable sleep retry interval appears to make parallel GET
 * requests perform much faster than using a fixed interval.
 */
inline constexpr int maxConcurrentArrowConversions = 36; // Set to ~#cores
inline constexpr int minimumSleepRetryTimeMS = 5;
inline constexpr int variableSleepRetryTimeMS = 15;

/**
 * System parameters
 */
inline bool USE_BLOOM_FILTER = false;
inline bool USE_ARROW_GROUP_BY_IMPL = true;
inline bool USE_ARROW_HASH_JOIN_IMPL = true;
inline bool USE_ARROW_BLOOM_FILTER_IMPL = true;
inline bool USE_TWO_PHASE_GROUP_BY = true;
inline bool USE_SHUFFLE_KERNEL_2 = true;
inline constexpr bool ENABLE_DIST_BCAST_BATCH_EXCHANGE = true;     // whether to batch exchange bcast data in dist join
inline constexpr bool ENABLE_DIST_SHUFFLE_BATCH_EXCHANGE = true;   // whether to batch exchange shuffle data in dist join
inline constexpr int64_t DIST_EXCHANGE_BATCH_SIZE = 10000;     // num rows in a batch when exchanging data in dist exec
inline constexpr bool ENABLE_PARALLEL_BATCH_EXCHANGE = false;  // use multiple actors to receive exchanged batches in a single node
                                                               // evaluation shows not beneficial for all queries, need to revisit
inline constexpr int BATCH_EXCHANGE_PARALLEL_DEGREE = 4;    // num of threads to read using a same flight client when doing batch exchange
inline constexpr bool ENABLE_SHUFFLE_PUSHDOWN_BATCH_LOAD = false;   // whether to batch results during shuffle pushdown,
                                                                    // evaluation shows not beneficial, need to revisit
inline constexpr bool USE_FLIGHT_COMM = true;
inline constexpr int64_t BLOOM_FILTER_MAX_INPUT_SIZE = 20000000;  // won't create bloom filter if input is too large,
                                                                  // only for vanilla bloom filter
inline constexpr bool SCAN_S3_PARQUET_PARTIAL_COLUMNS = true;    // currently using s3fs to read partial Parquet columns causes
                                                        // "AWS Error [code 15]: No response body."
inline join::DistJoinType DIST_JOIN_TYPE = join::DistJoinType::PTION;
inline constexpr bool USE_DOUBLE_EXEC_ADAPT = true;     // perform adapt exec based on cardinalities of the old run
                                                        // instead of real adapt exec
inline bool TEMP_FIX_TPCH_Q21 = false;   // BCAST hangs at TPC-H Q21 SF100

/**
 * Pushdown parameters used by FPDB store (co-located join is set in fpdb-plan)
 * Set by "pushdown.conf"
 * Basic pushdown features (e.g. filter, project, aggregate) are enabled by default
 */
inline bool ENABLE_GROUP_BY_PUSHDOWN;
inline bool ENABLE_SHUFFLE_PUSHDOWN;
inline bool ENABLE_BLOOM_FILTER_PUSHDOWN;
inline bool ENABLE_FILTER_BITMAP_PUSHDOWN;
inline bool ENABLE_ADAPTIVE_PUSHDOWN = false;     // we need to send flight request to storage side to enable this
static constexpr std::string_view PullupOpNamePrefix = "RemoteFileScan";
static constexpr std::string_view PushdownOpNamePrefix = "FPDBStoreSuper";

/**
 * For predicate transfer
 */
inline PredTransOrderType PRED_TRANS_ORDER_TYPE = PredTransOrderType::SMALL_TO_LARGE;
inline bool ENABLE_YANNAKAKIS = false;      // only used by BFSPredTransOrder
inline DistPredTransType DIST_PRED_TRANS_TYPE = DistPredTransType::BCAST_BF;
inline constexpr bool USE_DIST_GLOBAL_BF = true;  // when constructing global BF, construct a single one across
                                                  // the entire cluster (true), or each node constructs one (false)
inline constexpr bool USE_PARALLEL_DIST_GLOBAL_BF_MERGE = true;  // whether to perform dist global bf merge in parallel
inline bool PRUNE_PRED_TRANS = false;       // whether to prune unuseful pred-trans steps

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GLOBALS_H
