//
// Created by matt on 1/6/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_TEST_TESTUTIL_H
#define NORMAL_NORMAL_PUSHDOWN_TEST_TESTUTIL_H

#include <experimental/filesystem>
#include <normal/core/OperatorManager.h>
#include <normal/plan/LogicalPlan.h>

/*
 * Hack to get the current DocTest test being run. Need to forward declare getCurrentTestName declared in MainTest.
 */
const char *getCurrentTestName();

using namespace std::experimental;

namespace normal::pushdown::test {

class TestUtil {
public:
  static filesystem::path getTestScratchDirectory();
  static void writeExecutionPlan(core::OperatorManager &mgr);
  static void writeExecutionPlan(plan::LogicalPlan &plan);
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_TEST_TESTUTIL_H
