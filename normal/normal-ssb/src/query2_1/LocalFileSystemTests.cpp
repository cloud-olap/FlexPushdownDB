//
// Created by matt on 10/8/20.
//

#include "normal/ssb/query2_1/LocalFileSystemTests.h"

#include <doctest/doctest.h>
#include <filesystem>
#include <normal/ssb/Globals.h>
#include <normal/ssb/query2_1/LocalFileSystemQueries.h>
#include <normal/ssb/TestUtil.h>
#include <normal/core/Normal.h>
#include <normal/ssb/query2_1/SQL.h>

using namespace normal::ssb::query2_1;

void LocalFileSystemTests::partFilter(const std::string &category,
								  const std::string &dataDir,
									  FileType fileType,
								  int numConcurrentUnits,
								  bool check,
								  const std::shared_ptr<Normal> &n) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', category: {}, numConcurrentUnits: {}",
			  dataDir, category, numConcurrentUnits);

  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::partFilter(dataDir,fileType,
																				   category,
																			   numConcurrentUnits, n));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::partFilter(category, "temp"),
											{filesystem::absolute(dataDir + "/part.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
		CHECK_EQ(expected->size(), actual->numRows());
  }
}

void LocalFileSystemTests::join2x(const std::string &region,
								  const std::string &dataDir,
								  FileType fileType,
								  int numConcurrentUnits,
								  bool check,
								  const std::shared_ptr<Normal> &n) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', region: {}, numConcurrentUnits: {}",
			  dataDir, region, numConcurrentUnits);

  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::join2x(dataDir,fileType,
																			   region,
																			   numConcurrentUnits, n));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::join2x(region, "temp"),
											{filesystem::absolute(dataDir + "/supplier.tbl"),
											 filesystem::absolute(dataDir + "/lineorder.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
		CHECK_EQ(expected->size(), actual->numRows());
  }
}

void LocalFileSystemTests::join3x(const std::string &region,
								  const std::string &dataDir,
								  FileType fileType,
								  int numConcurrentUnits,
								  bool check,
								  const std::shared_ptr<Normal> &n) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', region: {}, numConcurrentUnits: {}",
			  dataDir, region, numConcurrentUnits);

  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::join3x(dataDir,fileType,
																			   region,
																			   numConcurrentUnits, n));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::join3x(region, "temp"),
											{filesystem::absolute(dataDir + "/supplier.tbl"),
											 filesystem::absolute(dataDir + "/lineorder.tbl"),
											 filesystem::absolute(dataDir + "/date.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
		CHECK_EQ(expected->size(), actual->numRows());
  }
}

void LocalFileSystemTests::join(const std::string &category,
								const std::string &region,
								const std::string &dataDir,
								FileType fileType,
								int numConcurrentUnits,
								bool check,
								const std::shared_ptr<Normal> &n) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', category: {}, region: {}, numConcurrentUnits: {}",
			  dataDir, category, region, numConcurrentUnits);

  auto actual = TestUtil::executeExecutionPlan2(LocalFileSystemQueries::join(dataDir,fileType,
																			 category, region,
																			 numConcurrentUnits, n));
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

  if (check) {
	auto expected = TestUtil::executeSQLite(SQL::join(category, region, "temp"),
											{filesystem::absolute(dataDir + "/supplier.tbl"),
											 filesystem::absolute(dataDir + "/lineorder.tbl"),
											 filesystem::absolute(dataDir + "/date.tbl"),
											 filesystem::absolute(dataDir + "/part.tbl")});
	SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
		CHECK_EQ(expected->size(), actual->numRows());
  }
}