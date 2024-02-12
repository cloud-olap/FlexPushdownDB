//
// Created by matt on 1/5/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_GLOBALS_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include <spdlog/spdlog.h>

namespace fpdb::tuple {

/**
 * Default number of records per arrow chunk when processing record batches
 */
inline int64_t DefaultChunkSize = 100000;

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_GLOBALS_H
