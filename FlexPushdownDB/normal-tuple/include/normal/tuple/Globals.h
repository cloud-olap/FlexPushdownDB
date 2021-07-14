//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_GLOBALS_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include <spdlog/spdlog.h>

namespace normal::tuple {

/**
 * Default number of records per arrow chunk when processing record batches
 */
inline size_t DefaultChunkSize = 100000;

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_GLOBALS_H
