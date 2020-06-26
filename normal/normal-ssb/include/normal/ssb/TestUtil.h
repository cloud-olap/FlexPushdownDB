//
// Created by matt on 1/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_TESTUTIL_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_TESTUTIL_H

#include <experimental/filesystem>
#include <normal/core/OperatorManager.h>
#include <normal/plan/LogicalPlan.h>
#include <normal/tuple/TupleSet2.h>

/*
 * Hack to get the current DocTest test being run. Need to forward declare getCurrentTestName declared in MainTest.
 */
const char *getCurrentTestName();

using namespace std::experimental;
using namespace normal::tuple;
using namespace normal::core;

namespace normal::ssb {

class TestUtil {

public:

  static filesystem::path getTestScratchDirectory();

  static void writeExecutionPlan(normal::core::OperatorManager &mgr);

  static void writeExecutionPlan(normal::plan::LogicalPlan &plan);

  static std::shared_ptr<TupleSet2> executeExecutionPlanTest(const std::shared_ptr<OperatorManager> &mgr);
};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_TESTUTIL_H
