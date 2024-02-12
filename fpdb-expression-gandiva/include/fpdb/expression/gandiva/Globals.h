//
// Created by matt on 27/4/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_SRC_GLOBALS_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_SRC_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include <spdlog/spdlog.h>

namespace fpdb::expression::gandiva {

extern std::mutex BigGlobalLock;

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_SRC_GLOBALS_H
