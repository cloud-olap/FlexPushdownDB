//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_TEST_GLOBALS_H
#define NORMAL_NORMAL_TUPLE_TEST_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include "spdlog/spdlog.h"

#define BACKWARD_HAS_BFD 1
#include <backward.hpp>

const char *getCurrentTestName();
const char *getCurrentTestSuiteName();

namespace normal::tuple::test {
}

#endif //NORMAL_NORMAL_TUPLE_TEST_GLOBALS_H
