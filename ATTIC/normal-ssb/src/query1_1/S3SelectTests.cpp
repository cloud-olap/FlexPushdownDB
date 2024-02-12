//
// Created by matt on 6/7/20.
//
//
//#include "normal/ssb/query1_1/S3SelectTests.h"
//
//#include <doctest/doctest.h>
//#include <experimental/filesystem>
//#include <normal/ssb/Globals.h>
//#include <normal/ssb/query1_1/S3SelectQueries.h>
//#include <normal/ssb/query1_1/SQL.h>
//#include <normal/ssb/TestUtil.h>
//
//using namespace normal::ssb;
//using namespace normal::ssb::query1_1;
//
//void S3SelectTests::dateScan(const std::string &s3ObjectDir,
//							 const std::string &dataDir,
//							 FileType fileType,
//							 int numConcurrentUnits,
//							 int numIterations,
//							 bool check,
//							 const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  s3ObjectDir: '{}', dataDir: '{}', numConcurrentUnits: {}, numIterations: {}",
//			  s3ObjectDir, dataDir, numConcurrentUnits, numIterations);
//
//  AWSClient client;
//  client.init();
//
//  std::vector<std::shared_ptr<TupleSet2>> actuals;
//  for (int i = 0; i < numIterations; ++i) {
//	auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::dateScanPullUp("pushdowndb", s3ObjectDir, fileType,
//																				  numConcurrentUnits, client, n));
//	SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//	actuals.push_back(actual);
//  }
//
//  std::shared_ptr<TupleSet2> lastActual;
//  for (const auto &actual: actuals) {
//	if (!lastActual) {
//	  lastActual = actual;
//	} else {
//		  CHECK_EQ(actual->numRows(), lastActual->numRows());
//	}
//  }
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::dateScan("temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/date.tbl")});
//	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
//		CHECK_EQ(expected->size(), lastActual->numRows());
//  }
//}
//
//void S3SelectTests::hybridDateFilter(short year,
//									 const std::string &s3ObjectDir,
//									 const std::string &dataDir,
//									 FileType fileType,
//									 int numConcurrentUnits,
//									 int numIterations,
//									 bool check,
//									 const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  s3ObjectDir: '{}', dataDir: '{}', numConcurrentUnits: {}, numIterations: {}",
//			  s3ObjectDir, dataDir, numConcurrentUnits, numIterations);
//
//  AWSClient client;
//  client.init();
//
//  std::vector<std::shared_ptr<TupleSet2>> actuals;
//  for (int i = 0; i < numIterations; ++i) {
//	auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::dateFilterHybrid("pushdowndb", s3ObjectDir,  fileType, year,
//																					numConcurrentUnits, client, n));
//	SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//	actuals.push_back(actual);
//  }
//
//  std::shared_ptr<TupleSet2> lastActual;
//  for (const auto &actual: actuals) {
//	if (!lastActual) {
//	  lastActual = actual;
//	} else {
//		  CHECK_EQ(actual->numRows(), lastActual->numRows());
//	}
//  }
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::dateScan("temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/date.tbl")});
//	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
//		CHECK_EQ(expected->size(), lastActual->numRows());
//  }
//}
//
///**
// * Tests that SQLLite and Normal produce the same output for date scan and filter component of query 1.1
// *
// * Only checking row count at moment
// */
//void S3SelectTests::dateFilter(short year,
//							   const std::string &s3ObjectDir,
//							   const std::string &dataDir,
//							   FileType fileType,
//							   int numConcurrentUnits,
//							   bool check,
//							   const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  year: {}, dataDir: '{}', numConcurrentUnits: {}",
//			  year, dataDir, numConcurrentUnits);
//
//  AWSClient client;
//  client.init();
//
//  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::dateFilterPullUp("pushdowndb", s3ObjectDir, fileType,
//																				  year,
//																				  numConcurrentUnits, client, n));
//  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::dateFilter(year, "temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/date.tbl")});
//	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
//		CHECK_EQ(expected->size(), actual->numRows());
//  }
//}
//
///**
// * Tests that SQLLite and Normal produce the same output for lineorder scan component of query 1.1
// *
// * Only checking row count at moment
// */
//void S3SelectTests::lineOrderScan(const std::string &s3ObjectDir,
//								  const std::string &dataDir,
//								  FileType fileType,
//								  int numConcurrentUnits,
//								  bool check,
//								  const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}",
//			  dataDir, numConcurrentUnits);
//
//  AWSClient client;
//  client.init();
//
//  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::lineOrderScanPullUp("pushdowndb", s3ObjectDir, fileType,
//																					 numConcurrentUnits, client, n));
//  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::lineOrderScan("temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl")});
//	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
//		CHECK_EQ(expected->size(), actual->numRows());
//  }
//}
//
///**
// * Tests that SQLLite and Normal produce the same output for lineorder scan and filter component of query 1.1
// *
// * Only checking row count at moment
// */
//void S3SelectTests::lineOrderFilter(short discount,
//									short quantity,
//									const std::string &s3ObjectDir,
//									const std::string &dataDir,
//									FileType fileType,
//									int numConcurrentUnits,
//									bool check,
//									const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', discount: {}, quantity: {}, numConcurrentUnits: {}",
//			  dataDir, discount, quantity, numConcurrentUnits);
//
//  AWSClient client;
//  client.init();
//
//  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::lineOrderFilterPullUp("pushdowndb",
//																					   s3ObjectDir, fileType,
//																					   discount,
//																					   quantity,
//																					   numConcurrentUnits,
//																					   client,
//																					   n));
//  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::lineOrderFilter(discount, quantity, "temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl")});
//	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
//		CHECK_EQ(expected->size(), actual->numRows());
//  }
//}
//
///**
// * Tests that SQLLite and Normal produce the same output for join component of query 1.1
// *
// * Only checking row count at moment
// */
//void S3SelectTests::join(short year,
//						 short discount,
//						 short quantity,
//						 const std::string &s3ObjectDir,
//						 const std::string &dataDir,
//						 FileType fileType,
//						 int numConcurrentUnits,
//						 bool check,
//						 const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
//			  dataDir, year, discount, quantity, numConcurrentUnits);
//
//  AWSClient client;
//  client.init();
//
//  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::joinPullUp("pushdowndb", s3ObjectDir, fileType,
//																			year, discount, quantity,
//																			numConcurrentUnits, client, n));
//  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::join(year, discount, quantity, "temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/date.tbl"),
//											 std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl")});
//	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
//		CHECK_EQ(expected->size(), actual->numRows());
//  }
//}
//
///**
// * Tests that SQLLite and Normal produce the same output for full query 1.1
// */
//void S3SelectTests::full(short year, short discount, short quantity,
//						 const std::string &s3ObjectDir, const std::string &dataDir,
//						 FileType fileType,
//						 int numConcurrentUnits,
//						 bool check,
//						 const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
//			  dataDir, year, discount, quantity, numConcurrentUnits);
//
//  AWSClient client;
//  client.init();
//
//  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::fullPullUp("pushdowndb", s3ObjectDir, fileType,
//																			year, discount, quantity,
//																			numConcurrentUnits, client, n));
//
//  auto actualName = actual->getColumnByIndex(0).value()->getName();
//  auto actualValue = actual->getColumnByIndex(0).value()->element(0).value()->value<int>();
//
//  SPDLOG_INFO("Actual  |  {} = {}", actualName, actualValue);
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::full(year, discount, quantity, "temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/date.tbl"),
//											 std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl")});
//	auto expectedName = expected->at(0).at(0).first;
//	auto expectedValue = std::stod(expected->at(0).at(0).second);
//	SPDLOG_INFO("Expected  |  {} = {}", expectedName, expectedValue);
//		CHECK_EQ(expectedName, actualName);
//		CHECK_EQ(expectedValue, actualValue);
//  }
//}
//
///**
// * Tests that SQLLite and Normal produce the same output for full query 1.1
// */
//void S3SelectTests::fullPushDown(short year, short discount, short quantity,
//								 const std::string &s3ObjectDir, const std::string &dataDir,
//								 FileType fileType,
//								 int numConcurrentUnits,
//								 bool check,
//								 const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
//			  dataDir, year, discount, quantity, numConcurrentUnits);
//
//  AWSClient client;
//  client.init();
//
//  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::fullPushDown("pushdowndb", s3ObjectDir, fileType,
//																			  year, discount, quantity,
//																			  numConcurrentUnits, client, n));
//
//  auto actualName = actual->getColumnByIndex(0).value()->getName();
//  auto actualValue = actual->getColumnByIndex(0).value()->element(0).value()->value<int>();
//
//  SPDLOG_INFO("Actual  |  {} = {}", actualName, actualValue);
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::full(year, discount, quantity, "temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/date.tbl"),
//											 std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl")});
//	auto expectedName = expected->at(0).at(0).first;
//	auto expectedValue = std::stod(expected->at(0).at(0).second);
//	SPDLOG_INFO("Expected  |  {} = {}", expectedName, expectedValue);
//		CHECK_EQ(expectedName, actualName);
//		CHECK_EQ(expectedValue, actualValue);
//  }
//}
//
///**
// * Tests that SQLLite and Normal produce the same output for full query 1.1
// */
//void S3SelectTests::hybrid(short year, short discount, short quantity,
//						   const std::string &s3ObjectDir, const std::string &dataDir,
//						   FileType fileType,
//						   int numConcurrentUnits,
//						   bool check,
//						   const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
//			  dataDir, year, discount, quantity, numConcurrentUnits);
//
//  AWSClient client;
//  client.init();
//
//  auto actual = TestUtil::executeExecutionPlan2(S3SelectQueries::fullHybrid("pushdowndb", s3ObjectDir,fileType,
//																			year, discount, quantity,
//																			numConcurrentUnits, client, n));
//
//  auto actualName = actual->getColumnByIndex(0).value()->getName();
//  auto actualValue = actual->getColumnByIndex(0).value()->element(0).value()->value<int>();
//
//  SPDLOG_INFO("Actual  |  {} = {}", actualName, actualValue);
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::full(year, discount, quantity, "temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/date.tbl"),
//											 std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl")});
//	auto expectedName = expected->at(0).at(0).first;
//	auto expectedValue = std::stod(expected->at(0).at(0).second);
//	SPDLOG_INFO("Expected  |  {} = {}", expectedName, expectedValue);
//		CHECK_EQ(expectedName, actualName);
//		CHECK_EQ(expectedValue, actualValue);
//  }
//}