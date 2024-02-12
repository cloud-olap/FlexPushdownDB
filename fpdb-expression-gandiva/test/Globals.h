//
// Created by matt on 8/5/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_TEST_GLOBALS_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_TEST_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include "spdlog/spdlog.h"

#define BACKWARD_HAS_BFD 1
#include <backward.hpp>

namespace fpdb::expression::gandiva::test {
}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_TEST_GLOBALS_H
