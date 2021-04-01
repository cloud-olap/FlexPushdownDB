//
// Created by matt on 5/3/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GLOBALS_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL

#include <aws/s3/S3Client.h>
#include "AWSClient.h"
#include "spdlog/spdlog.h"

namespace normal::pushdown {

/**
 * Default number of tuples operators should buffer before sending to consumers
 */
inline constexpr int DefaultBufferSize = 10000;
inline constexpr int DefaultS3ScanBufferSize = 100000;
inline constexpr int DefaultS3ConversionBufferSize = 128 * 1024;
inline constexpr uint64_t DefaultS3RangeSize = 15 * 1024 * 1024; // 15MB/s This value was tuned on c5n.9xlarge and
                                                 // may need to be retuned for different instances with many more cores
inline constexpr bool RefinedWeightFunction = true;
inline constexpr double vNetwork = 0.24419;     // unit: GB/s
inline constexpr double vS3Scan = 3.88424;      // unit: GB/s
inline constexpr double vS3Filter = 0.084907;   // unit: GPred/s
inline size_t NetworkLimit = 0;

// These parameters are for running GET in parallel as a detached operation
// We only want to convert ~max cores results at a time since otherwise we get very bad cache thrashing
// that degrades system performance. Additionally setting a variable sleep retry interval appears to make parallel GET
// requests perform much faster than using a fixed interval.
inline constexpr int maxConcurrentArrowConversions = 36; // Set to ~#cores
inline constexpr int minimumSleepRetryTimeMS = 5;
inline constexpr int variableSleepRetryTimeMS = 15;
}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GLOBALS_H
