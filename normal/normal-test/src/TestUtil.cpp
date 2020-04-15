//
// Created by matt on 13/4/20.
//

#include <normal/test/TestUtil.h>

using namespace std::experimental;
using namespace normal::test;

filesystem::path TestUtil::getTestScratchDirectory() {
  auto testName = getCurrentTestName();
  auto currentPath = filesystem::current_path();
  auto baseTestScratchDir = currentPath.append(std::string("tests"));
  filesystem::create_directories(baseTestScratchDir);
  auto testScratchDir = baseTestScratchDir.append(testName);
  filesystem::create_directories(testScratchDir);
  return testScratchDir;
}

void TestUtil::writeLogicalExecutionPlan(normal::core::OperatorManager &mgr) {
  auto testScratchDir = getTestScratchDirectory();
  auto planFile = testScratchDir.append("logical-execution-plan.svg");
  mgr.write_graph(planFile);
}
