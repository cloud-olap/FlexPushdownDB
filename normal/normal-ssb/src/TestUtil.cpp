//
// Created by matt on 1/6/20.
//

#include "normal/ssb/TestUtil.h"

#include <doctest/doctest.h>

#include <normal/pushdown/Collate.h>
#include <normal/ssb/SQLite3.h>

using namespace normal::ssb;
using namespace normal::pushdown;

filesystem::path TestUtil::getTestScratchDirectory() {
  auto testName = getCurrentTestName();
  auto testSuite = getCurrentTestSuiteName();
  auto currentPath = filesystem::current_path();
  auto baseTestScratchDir = currentPath.append(std::string("tests"));
  filesystem::create_directories(baseTestScratchDir);
  auto testSuiteScratchDir = baseTestScratchDir.append(testSuite);
  auto testScratchDir = testSuiteScratchDir.append(testName);
  filesystem::create_directories(testScratchDir);
  return testScratchDir;
}

void TestUtil::writeExecutionPlan(normal::core::OperatorManager &mgr) {
  auto testScratchDir = getTestScratchDirectory();

  auto physicalPlanFile = filesystem::path(testScratchDir).append("physical-execution-plan.svg");
//  mgr.write_graph(physicalPlanFile);
}

void TestUtil::writeExecutionPlan2(OperatorGraph &g) {
  auto testScratchDir = getTestScratchDirectory();

  auto physicalPlanFile = filesystem::path(testScratchDir).append("physical-execution-plan.svg");
//  g.write_graph(physicalPlanFile);
}

void TestUtil::writeExecutionPlan(normal::plan::LogicalPlan &plan) {
  auto testScratchDir = getTestScratchDirectory();

  auto logicalPlanFile = filesystem::path(testScratchDir).append("logical-execution-plan.svg");
//  plan.writeGraph(logicalPlanFile);
}

std::shared_ptr<TupleSet2> TestUtil::executeExecutionPlanTest2(const std::shared_ptr<OperatorGraph> &g) {

  TestUtil::writeExecutionPlan2(*g);

  g->boot();
  g->start();
  g->join();

  auto tuples = std::static_pointer_cast<Collate>(g->getOperator(fmt::format("/query-{}/collate", g->getId())))->tuples();

  SPDLOG_INFO("Metrics:\n{}", g->showMetrics());

  auto tupleSet = TupleSet2::create(tuples);
  return tupleSet;
}

/**
 * Runs the given query in sql lite, returning the results or failing the test on an error
 */
//std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>>
//TestUtil::executeSQLite(const std::string &sql, std::vector<std::string> dataFiles) {
//
//  std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>> expected;
//  auto expectedSQLite3Results = SQLite3::execute(sql, dataFiles);
//  if (!expectedSQLite3Results.has_value()) {
//	FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
//  } else {
//	expected = expectedSQLite3Results.value();
//  }
//
//  return expected;
//}

/**
 * Runs the given Normal execution plan, returning the results or failing the test on an error
 */
std::shared_ptr<TupleSet2> TestUtil::executeExecutionPlan2(const std::shared_ptr<OperatorGraph> &g) {
  auto tupleSet = TestUtil::executeExecutionPlanTest2(g);
  SPDLOG_DEBUG("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  return tupleSet;
}
