//
// Created by matt on 6/7/20.
//
//
//#include "normal/ssb/query1_1/LocalFileSystemTests.h"
//
//#include <doctest/doctest.h>
//#include <experimental/filesystem>
//#include <normal/ssb/Globals.h>
//#include <normal/ssb/query1_1/LocalFileSystemQueries.h>
//#include <normal/ssb/query1_1/SQL.h>
//#include <normal/ssb/TestUtil.h>
//#include <normal/core/ATTIC/Normal.h>
//
//using namespace normal::ssb;
//using namespace normal::ssb::query1_1;
//
//void LocalFileSystemTests::dateScan(const std::string &dataDir, FileType fileType, int numConcurrentUnits, int numIterations, bool check, const std::shared_ptr<Normal>& n) {
//
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}, numIterations: {}",
//			  dataDir, numConcurrentUnits, numIterations);
//
//  std::vector<std::shared_ptr<TupleSet2>> actuals;
//  for(int i= 0;i<numIterations;++i) {
//	auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::dateScan(dataDir, fileType,
//																				   numConcurrentUnits, n));
//	SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//	actuals.push_back(actual);
//  }
//
//  std::shared_ptr<TupleSet2> lastActual;
//  for(const auto &actual: actuals){
//	if(!lastActual) {
//	  lastActual = actual;
//	}
//	else {
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
//void LocalFileSystemTests::lineOrderScan(const std::string &dataDir, FileType fileType, int numConcurrentUnits, int numIterations, bool check, const std::shared_ptr<Normal>& n) {
//
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}, numIterations: {}",
//			  dataDir, numConcurrentUnits, numIterations);
//
//  std::vector<std::shared_ptr<TupleSet2>> actuals;
//  for(int i= 0;i<numIterations;++i) {
//	auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::lineOrderScan(dataDir, fileType,
//																				   numConcurrentUnits, n));
//	SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//	actuals.emplace_back(actual);
//  }
//
//  std::shared_ptr<TupleSet2> lastActual;
//  for(const auto &actual: actuals){
//	if(!lastActual) {
//	  lastActual = actual;
//	}
//	else {
//		  CHECK_EQ(actual->numRows(), lastActual->numRows());
//	}
//  }
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::lineOrderScan("temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl")});
//	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
//		CHECK_EQ(expected->size(), lastActual->numRows());
//  }
//}
//
///**
// * LocalFileSystemTests that SQLLite and Normal produce the same output for date scan and filter component of query 1.1
// *
// * Only checking row count at moment
// */
//void LocalFileSystemTests::dateFilter(short year, const std::string &dataDir, FileType fileType, int numConcurrentUnits, bool check, const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  year: {}, dataDir: '{}', numConcurrentUnits: {}",
//			  dataDir, year, numConcurrentUnits);
//
//  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::dateFilter(dataDir, fileType,
//																				  year,
//																				  numConcurrentUnits, n));
//  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::dateFilter(year, "temp"),
//								  {std::experimental::filesystem::absolute(dataDir + "/date.tbl")});
//	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
//	CHECK_EQ(expected->size(), actual->numRows());
//  }
//}
//
///**
// * LocalFileSystemTests that SQLLite and Normal produce the same output for lineorder scan and filter component of query 1.1
// *
// * Only checking row count at moment
// */
//void LocalFileSystemTests::lineOrderFilter(short discount, short quantity, const std::string &dataDir, FileType fileType, int numConcurrentUnits, bool check, const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', discount: {}, quantity: {}, numConcurrentUnits: {}",
//			  dataDir, discount, quantity, numConcurrentUnits);
//
//  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::lineOrderFilter(dataDir,
//																						fileType,
//																					   discount, quantity,
//																					   numConcurrentUnits, n));
//  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::lineOrderFilter(discount, quantity, "temp"),
//								  {std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl")});
//	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
//	CHECK_EQ(expected->size(), actual->numRows());
//  }
//}
//
///**
// * LocalFileSystemTests that SQLLite and Normal produce the same output for join component of query 1.1
// *
// * Only checking row count at moment
// */
//void LocalFileSystemTests::join(short year, short discount, short quantity, const std::string &dataDir, FileType fileType, int numConcurrentUnits, bool check, const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
//			  dataDir, year, discount, quantity, numConcurrentUnits);
//
//  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::join(dataDir, fileType,
//																			year, discount, quantity,
//																			numConcurrentUnits, n));
//  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::join(year, discount, quantity, "temp"),
//								  {std::experimental::filesystem::absolute(dataDir + "/date.tbl"),
//								   std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl")});
//	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
//	CHECK_EQ(expected->size(), actual->numRows());
//  }
//}
//
//void LocalFileSystemTests::full2(short year, short discount, short quantity,
//								const std::string &dataDir,
//								 FileType fileType,
//								int numConcurrentUnits,
//								int numIterations,
//								bool check,
//								 const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
//			  dataDir, year, discount, quantity, numConcurrentUnits);
//
//  std::vector<std::pair<std::string, double>> actuals;
//  for(int i= 0;i<numIterations;++i) {
//	auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::full(dataDir, fileType,
//																			   year, discount, quantity,
//																			   numConcurrentUnits, n));
//
//	auto actualName = actual->getColumnByIndex(0).value()->getName();
//	auto actualValue = actual->getColumnByIndex(0).value()->element(0).value()->value<double>();
//
//	SPDLOG_INFO("Actual  |  {} = {}", actualName, actualValue);
//	actuals.emplace_back(actualName, actualValue);
//  }
//
//  std::pair<std::string, double> lastActual{std::string(""), -1};
//  for(const auto &actual: actuals){
//	if(lastActual.first == std::string("")) {
//	  lastActual.first = actual.first;
//	  lastActual.second = actual.second;
//	}
//	else {
//		  CHECK_EQ(actual.first, lastActual.first);
//		  CHECK_EQ(actual.second, lastActual.second);
//	}
//  }
//
//  if (check) {
//	auto expected = TestUtil::executeSQLite(SQL::full(year, discount, quantity, "temp"),
//											{std::experimental::filesystem::absolute(dataDir + "/date.tbl"),
//											 std::experimental::filesystem::absolute(dataDir + "/lineorder.tbl")});
//	auto expectedName = expected->at(0).at(0).first;
//	auto expectedValue = std::stod(expected->at(0).at(0).second);
//	SPDLOG_INFO("Expected  |  {} = {}", expectedName, expectedValue);
//		CHECK_EQ(expectedName, lastActual.first);
//		CHECK_EQ(expectedValue, lastActual.second);
//  }
//}
//
//void LocalFileSystemTests::bloom(short year, short discount, short quantity,
//								 const std::string &dataDir,
//								 FileType fileType,
//								 int numConcurrentUnits,
//								 bool check,
//								 const std::shared_ptr<Normal>& n) {
//
//  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
//			  dataDir, year, discount, quantity, numConcurrentUnits);
//
//  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::bloom(dataDir, fileType,
//																			 year, discount, quantity,
//																			 numConcurrentUnits, n));
//
//  auto actualName = actual->getColumnByIndex(0).value()->getName();
//  auto actualValue = actual->getColumnByIndex(0).value()->element(0).value()->value<double>();
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