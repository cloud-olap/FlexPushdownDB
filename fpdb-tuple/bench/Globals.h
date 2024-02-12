//
// Created by matt on 20/5/20.
//

#ifndef FPDB_FPDB_TUPLE_TEST_GLOBALS_H
#define FPDB_FPDB_TUPLE_TEST_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include "spdlog/spdlog.h"

#define BACKWARD_HAS_BFD 1
#include <backward.hpp>

const char *getCurrentTestName();
const char *getCurrentTestSuiteName();

namespace fpdb::tuple::test {
}

#endif //FPDB_FPDB_TUPLE_TEST_GLOBALS_H
