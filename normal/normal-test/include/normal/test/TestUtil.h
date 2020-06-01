//
// Created by matt on 13/4/20.
//

#ifndef NORMAL_NORMAL_TEST_INCLUDE_NORMAL_TEST_TESTUTIL_H
#define NORMAL_NORMAL_TEST_INCLUDE_NORMAL_TEST_TESTUTIL_H

#include <experimental/filesystem>

#include <normal/core/OperatorManager.h>
#include <normal/sql/Interpreter.h>

/*
 * Hack to get the current DocTest test being run. Need to forward declare getCurrentTestName declared in MainTest.
 */
const char *getCurrentTestName();

namespace normal::test {

class TestUtil {
public:

  /**
   * Finds and creates a directory for the current test at <working-directory>/normal-test/tests/<test-name>
   *
   * <working-directory> is CMAKE_CURRENT_BINARY_DIR when running from IDE
   *
   * @return
   */
  static std::experimental::filesystem::path getTestScratchDirectory();

  /**
   * Generates and saves an SVG of the logical and physical execution plans to the test scratch directory
   * @param mgr
   */
  static void writeExecutionPlan(normal::sql::Interpreter &i);

  static void writeExecutionPlan(core::OperatorManager &mgr);
};

}

#endif //NORMAL_NORMAL_TEST_INCLUDE_NORMAL_TEST_TESTUTIL_H
