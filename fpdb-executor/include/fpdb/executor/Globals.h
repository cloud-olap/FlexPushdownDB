//
// Created by Yifei Yang on 11/17/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_GLOBALS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_GLOBALS_H

#include <mutex>

namespace fpdb::executor {

inline std::mutex ConcurrentOutputMutex;    // used for output of concurrent runs
inline constexpr bool PrepareExecutionInParallel = true;    // whether to spawn actors ("boot()", "start()") in parallel

// Clear global states
void clearGlobal();

// If using adaptive exec
bool enableAdaptExec();

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_GLOBALS_H
