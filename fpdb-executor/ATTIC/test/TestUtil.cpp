//
// Created by matt on 1/6/20.
//

#include "TestUtil.h"


using namespace fpdb::pushdown::test;


filesystem::path TestUtil::getTestScratchDirectory() {
  auto testName = getCurrentTestName();
  auto currentPath = filesystem::current_path();
  auto baseTestScratchDir = currentPath.append(std::string("tests"));
  filesystem::create_directories(baseTestScratchDir);
  auto testScratchDir = baseTestScratchDir.append(testName);
  filesystem::create_directories(testScratchDir);
  return testScratchDir;
}

void TestUtil::writeExecutionPlan(OperatorGraph &g) {
  auto testScratchDir = getTestScratchDirectory();

  auto physicalPlanFile = filesystem::path(testScratchDir).append("physical-execution-plan.svg");
//  g.write_graph(physicalPlanFile);
}

void TestUtil::writeExecutionPlan(fpdb::plan::LogicalPlan &plan) {
  auto testScratchDir = getTestScratchDirectory();

  auto logicalPlanFile = filesystem::path(testScratchDir).append("logical-execution-plan.svg");
//  plan.writeGraph(logicalPlanFile);
}