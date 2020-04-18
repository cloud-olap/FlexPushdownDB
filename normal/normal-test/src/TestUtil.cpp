//
// Created by matt on 13/4/20.
//

#include <normal/test/TestUtil.h>
#include <normal/sql/Interpreter.h>

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

void TestUtil::writeExecutionPlan(normal::sql::Interpreter &i) {
  auto testScratchDir = getTestScratchDirectory();

  auto logicalPlanFile = filesystem::path(testScratchDir).append("logical-execution-plan.svg");
  i.getLogicalPlan()->writeGraph(logicalPlanFile);

  auto physicalPlanFile = filesystem::path(testScratchDir).append("physical-execution-plan.svg");
  i.getOperatorManager()->write_graph(physicalPlanFile);
}

std::string TestUtil::showMetrics(normal::core::OperatorManager &mgr) {

  std::stringstream ss;

  auto operators = mgr.getOperators();

  long totalProcessingTime = 0;
  for (auto &entry : operators) {
	auto processingTime = entry.second->operatorActor()->getProcessingTime();
	totalProcessingTime += processingTime;
  }

  ss << "Operator Processing Time" << std::endl;

  ss << std::left << std::setw(80) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');

  for (auto &entry : operators) {
	auto operatorName = entry.second->op()->name();
	auto processingTime = entry.second->operatorActor()->getProcessingTime();
	auto processingFraction = (double)processingTime / (double)totalProcessingTime;
	std::stringstream formattedProcessingTime;
	formattedProcessingTime << processingTime << " \u33B1";
	std::stringstream formattedProcessingPercentage;
	formattedProcessingPercentage << "(" << (processingFraction * 100.0) << "%)";
	ss << std::left << std::setw(50) << "'" + operatorName + "'";
	ss << std::left << std::setw(15) << formattedProcessingTime.str();
	ss << std::left << std::setw(15) << formattedProcessingPercentage.str();
	ss << std::endl;
  }

  ss << std::left << std::setw(80) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');

  ss << std::left << std::setw(50) << "Total operator processing time";
  std::stringstream formattedProcessingTime;
  formattedProcessingTime << totalProcessingTime << " \u33B1";
  std::stringstream formattedProcessingPercentage;
  formattedProcessingPercentage << "(" << 100.0 << "%)";
  ss << std::left << std::setw(15) << formattedProcessingTime.str();
  ss << std::left << std::setw(15) << formattedProcessingPercentage.str();

  return ss.str();
}
