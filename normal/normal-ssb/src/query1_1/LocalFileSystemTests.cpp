//
// Created by matt on 6/7/20.
//

#include "normal/ssb/query1_1/LocalFileSystemTests.h"

#include <doctest/doctest.h>
#include <filesystem>
#include <normal/ssb/Globals.h>
#include <normal/ssb/query1_1/LocalFileSystemQueries.h>
#include <normal/ssb/query1_1/SQL.h>
#include <normal/ssb/TestUtil.h>

using namespace normal::ssb;
using namespace normal::ssb::query1_1;

void LocalFileSystemTests::dateScan(const std::string &dataDir, int numConcurrentUnits, int numIterations, bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}, numIterations: {}",
			  dataDir, numConcurrentUnits, numIterations);

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  std::vector<std::shared_ptr<TupleSet2>> actuals;
  for(int i= 0;i<numIterations;++i) {
	auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::dateScan(dataDir,
																				   numConcurrentUnits, mgr));
	SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
	actuals.emplace_back(actual);
  }

  mgr->stop();

  std::shared_ptr<TupleSet2> lastActual;
  for(const auto &actual: actuals){
    if(!lastActual) {
	  lastActual = actual;
	}
    else {
      CHECK_EQ(actual->numRows(), lastActual->numRows());
	}
  }

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::dateScan("temp"),
											{std::filesystem::absolute(dataDir + "/date.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
		CHECK_EQ(expected->size(), lastActual->numRows());
  }
}

/**
 * LocalFileSystemTests that SQLLite and Normal produce the same output for date scan and filter component of query 1.1
 *
 * Only checking row count at moment
 */
void LocalFileSystemTests::dateFilter(short year, const std::string &dataDir, int numConcurrentUnits, bool check) {

  SPDLOG_INFO("Arguments  |  year: {}, dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, year, numConcurrentUnits);

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::dateFilter(dataDir,
																				  year,
																				  numConcurrentUnits, mgr));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  mgr->stop();

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::dateFilter(year, "temp"),
								  {std::filesystem::absolute(dataDir + "/date.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
	CHECK_EQ(expected->size(), actual->numRows());
  }
}

/**
 * LocalFileSystemTests that SQLLite and Normal produce the same output for lineorder scan component of query 1.1
 *
 * Only checking row count at moment
 */
void LocalFileSystemTests::lineOrderScan(const std::string &dataDir, int numConcurrentUnits, bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, numConcurrentUnits);

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::lineOrderScan(dataDir,
																					 numConcurrentUnits, mgr));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  mgr->stop();

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::lineOrderScan("temp"),
								  {filesystem::absolute(dataDir + "/lineorder.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
	CHECK_EQ(expected->size(), actual->numRows());
  }
}

/**
 * LocalFileSystemTests that SQLLite and Normal produce the same output for lineorder scan and filter component of query 1.1
 *
 * Only checking row count at moment
 */
void LocalFileSystemTests::lineOrderFilter(short discount, short quantity, const std::string &dataDir, int numConcurrentUnits, bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, discount, quantity, numConcurrentUnits);

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::lineOrderFilter(dataDir,
																					   discount, quantity,
																					   numConcurrentUnits, mgr));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  mgr->stop();

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::lineOrderFilter(discount, quantity, "temp"),
								  {filesystem::absolute(dataDir + "/lineorder.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
	CHECK_EQ(expected->size(), actual->numRows());
  }
}

/**
 * LocalFileSystemTests that SQLLite and Normal produce the same output for join component of query 1.1
 *
 * Only checking row count at moment
 */
void LocalFileSystemTests::join(short year, short discount, short quantity, const std::string &dataDir, int numConcurrentUnits, bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::join(dataDir,
																			year, discount, quantity,
																			numConcurrentUnits, mgr));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  mgr->stop();

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::join(year, discount, quantity, "temp"),
								  {filesystem::absolute(dataDir + "/date.tbl"),
								   filesystem::absolute(dataDir + "/lineorder.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
	CHECK_EQ(expected->size(), actual->numRows());
  }
}

/**
 * LocalFileSystemTests that SQLLite and Normal produce the same output for full query 1.1
 */
void LocalFileSystemTests::full(short year, short discount, short quantity,
								const std::string &dataDir,
								int numConcurrentUnits,
								bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::full(dataDir,
																			year, discount, quantity,
																			numConcurrentUnits, mgr));

  auto actualName = actual->getColumnByIndex(0).value()->getName();
  auto actualValue = actual->getColumnByIndex(0).value()->element(0).value()->value<int>();

  SPDLOG_INFO("Actual  |  {} = {}", actualName, actualValue);

  mgr->stop();

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::full(year, discount, quantity, "temp"),
								  {filesystem::absolute(dataDir + "/date.tbl"),
								   filesystem::absolute(dataDir + "/lineorder.tbl")});
	auto expectedName = expected->at(0).at(0).first;
	auto expectedValue = std::stod(expected->at(0).at(0).second);
	SPDLOG_INFO("Expected  |  {} = {}", expectedName, expectedValue);
	CHECK_EQ(expectedName, actualName);
	CHECK_EQ(expectedValue, actualValue);
  }
}