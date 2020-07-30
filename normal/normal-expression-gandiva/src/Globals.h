//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_SRC_GLOBALS_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_SRC_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include <spdlog/spdlog.h>

namespace normal::expression::gandiva {

extern std::mutex BigGlobalLock;

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_SRC_GLOBALS_H
