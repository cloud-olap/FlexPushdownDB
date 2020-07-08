//
// Created by matt on 6/7/20.
//

#include "normal/ssb/query1_1/S3SelectTests.h"

#include <doctest/doctest.h>
#include <filesystem>
#include <normal/ssb/Globals.h>
#include <normal/ssb/query1_1/S3SelectQueries.h>
#include <normal/ssb/query1_1/SQL.h>
#include <normal/ssb/TestUtil.h>

using namespace normal::ssb;
using namespace normal::ssb::query1_1;

/**
 * Tests that SQLLite and Normal produce the same output for date scan component of query 1.1
 *
 * Only checking row count at moment
 */
void S3SelectTests::dateScan(const std::string &s3ObjectDir,
							 const std::string &dataDir,
							 int numConcurrentUnits,
							 bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, numConcurrentUnits);

  AWSClient client;
  client.init();

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::dateScanPullUp("s3filter", s3ObjectDir,
																			   numConcurrentUnits, client, mgr));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  mgr->stop();

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::dateScan("temp"),
											{std::filesystem::absolute(dataDir + "/date.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
		CHECK_EQ(expected->size(), actual->numRows());
  }
}

/**
 * Tests that SQLLite and Normal produce the same output for date scan and filter component of query 1.1
 *
 * Only checking row count at moment
 */
void S3SelectTests::dateFilter(short year,
							   const std::string &s3ObjectDir,
							   const std::string &dataDir,
							   int numConcurrentUnits,
							   bool check) {

  SPDLOG_INFO("Arguments  |  year: {}, dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, year, numConcurrentUnits);

  AWSClient client;
  client.init();

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::dateFilterPullUp("s3filter", s3ObjectDir,
																				 year,
																				 numConcurrentUnits, client, mgr));
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
 * Tests that SQLLite and Normal produce the same output for lineorder scan component of query 1.1
 *
 * Only checking row count at moment
 */
void S3SelectTests::lineOrderScan(const std::string &s3ObjectDir,
								  const std::string &dataDir,
								  int numConcurrentUnits,
								  bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, numConcurrentUnits);

  AWSClient client;
  client.init();

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::lineOrderScanPullUp("s3filter", s3ObjectDir,
																					numConcurrentUnits, client, mgr));
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
 * Tests that SQLLite and Normal produce the same output for lineorder scan and filter component of query 1.1
 *
 * Only checking row count at moment
 */
void S3SelectTests::lineOrderFilter(short discount,
									short quantity,
									const std::string &s3ObjectDir,
									const std::string &dataDir,
									int numConcurrentUnits,
									bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, discount, quantity, numConcurrentUnits);

  AWSClient client;
  client.init();

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::lineOrderFilterPullUp("s3filter", s3ObjectDir,
																					  discount, quantity,
																					  numConcurrentUnits, client, mgr));
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
 * Tests that SQLLite and Normal produce the same output for join component of query 1.1
 *
 * Only checking row count at moment
 */
void S3SelectTests::join(short year,
						 short discount,
						 short quantity,
						 const std::string &s3ObjectDir,
						 const std::string &dataDir,
						 int numConcurrentUnits,
						 bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  AWSClient client;
  client.init();

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::joinPullUp("s3filter", s3ObjectDir,
																		   year, discount, quantity,
																		   numConcurrentUnits, client, mgr));
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
 * Tests that SQLLite and Normal produce the same output for full query 1.1
 */
void S3SelectTests::full(short year, short discount, short quantity,
						 const std::string &s3ObjectDir, const std::string &dataDir,
						 int numConcurrentUnits,
						 bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  AWSClient client;
  client.init();

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::fullPullUp("s3filter", s3ObjectDir,
																		   year, discount, quantity,
																		   numConcurrentUnits, client, mgr));

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

/**
 * Tests that SQLLite and Normal produce the same output for full query 1.1
 */
void S3SelectTests::fullPushDown(short year, short discount, short quantity,
								 const std::string &s3ObjectDir, const std::string &dataDir,
								 int numConcurrentUnits,
								 bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  AWSClient client;
  client.init();

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::fullPushDown("s3filter", s3ObjectDir,
																			 year, discount, quantity,
																			 numConcurrentUnits, client, mgr));

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

/**
 * Tests that SQLLite and Normal produce the same output for full query 1.1
 */
void S3SelectTests::hybrid(short year, short discount, short quantity,
						   const std::string &s3ObjectDir, const std::string &dataDir,
						   int numConcurrentUnits,
						   bool check) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  AWSClient client;
  client.init();

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::fullHybrid("s3filter", s3ObjectDir,
																		   year, discount, quantity,
																		   numConcurrentUnits, client, mgr));

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