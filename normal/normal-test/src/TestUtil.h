//
// Created by matt on 13/4/20.
//

#ifndef NORMAL_NORMAL_TEST_SRC_TESTUTIL_H
#define NORMAL_NORMAL_TEST_SRC_TESTUTIL_H

#include <experimental/filesystem>

#include "normal/core/OperatorManager.h"

/*
 * Hack to get the current DocTest test being run. Need to forward declare getCurrentTestName declared in MainTest.
 */
const char *getCurrentTestName();

class TestUtil {
public:

  static std::experimental::filesystem::path getTestScratchDirectory() {
    auto testName = getCurrentTestName();
    auto currentPath = std::experimental::filesystem::current_path();
    auto baseTestScratchDir = currentPath.append("tests");
    std::experimental::filesystem::create_directories(baseTestScratchDir);
    auto testScratchDir = baseTestScratchDir.append(testName);
    std::experimental::filesystem::create_directories(testScratchDir);
    return testScratchDir;
  }

  static void writeLogicalExecutionPlan(normal::core::OperatorManager &mgr) {
    auto testScratchDir = getTestScratchDirectory();
    auto planFile = testScratchDir.append("logical-execution-plan.svg");
    mgr.write_graph(planFile);
  }
};

#endif //NORMAL_NORMAL_TEST_SRC_TESTUTIL_H
