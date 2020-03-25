//
// Created by matt on 5/3/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GLOBALS_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

/**
 * Setting the log level here will disable macros for levels below it
 */
#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include "spdlog/spdlog.h"

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GLOBALS_H
