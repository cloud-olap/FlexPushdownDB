//
// Created by matt on 1/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_TESTUTIL_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_TESTUTIL_H

#include <experimental/filesystem>
#include <normal/core/OperatorManager.h>
#include <normal/plan/LogicalPlan.h>

/*
 * Hack to get the current DocTest test being run. Need to forward declare getCurrentTestName declared in MainTest.
 */
const char *getCurrentTestName();

using namespace std::experimental;

namespace normal::ssb {

class TestUtil {

public:

  static filesystem::path getTestScratchDirectory();

  static void writeExecutionPlan(normal::core::OperatorManager &mgr);

  static void writeExecutionPlan(normal::plan::LogicalPlan &plan);
};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_TESTUTIL_H
