//
// Created by matt on 1/6/20.
//

#ifndef FPDB_FPDB_PUSHDOWN_TEST_TESTUTIL_H
#define FPDB_FPDB_PUSHDOWN_TEST_TESTUTIL_H

#include <experimental/filesystem>
#include <fpdb/core/OperatorManager.h>
#include <fpdb/plan/LogicalPlan.h>
#include <fpdb/core/graph/OperatorGraph.h>

/*
 * Hack to get the current DocTest test being run. Need to forward declare getCurrentTestName declared in MainTest.
 */
const char *getCurrentTestName();

using namespace std::experimental;
using namespace fpdb::core::graph;

namespace fpdb::pushdown::test {

class TestUtil {
public:
  static filesystem::path getTestScratchDirectory();
  static void writeExecutionPlan(OperatorGraph &g);
  static void writeExecutionPlan(plan::LogicalPlan &plan);
};

}

#endif //FPDB_FPDB_PUSHDOWN_TEST_TESTUTIL_H
