//
// Created by matt on 19/5/20.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_GLOBALS_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include "spdlog/spdlog.h"

namespace fpdb::cache {

/**
 * Whether to fix cache layout, i.e. not admit new data into the cache and evict data from the cache
 */
inline bool FIX_CACHE_LAYOUT = false;

}

#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_GLOBALS_H
