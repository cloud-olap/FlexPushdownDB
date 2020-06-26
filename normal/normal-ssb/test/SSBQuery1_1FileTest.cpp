//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>

#include <normal/ssb/TestUtil.h>
#include <normal/sql/Interpreter.h>
#include <normal/connector/local-fs/LocalFileSystemConnector.h>
#include <normal/connector/local-fs/LocalFileExplicitPartitioningScheme.h>
#include <normal/connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
#include <normal/connector/s3/S3SelectCatalogueEntry.h>
#include <normal/pushdown/Collate.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/ssb/SQLite3.h>

#include "normal/ssb/query1_1/Queries.h"
#include "normal/ssb/query1_1/SQL.h"

using namespace normal::ssb;
using namespace normal::ssb::query1_1;
using namespace normal::pushdown;
using namespace normal::tuple;


#define SKIP_SUITE false

TEST_SUITE ("ssb-query1.1-file" * doctest::skip(SKIP_SUITE)) {

/**
 * Tests that SQLLite and Normal produce the same output for date scan and filter component of query 1.1
 *
 * Only checking row count at moment
 */
TEST_CASE ("ssb-benchmark-ep-query1_1-datefilter-file-pullup" * doctest::skip(false || SKIP_SUITE)) {

  short year = 1992;
  std::string dataDir = "data/ssb-sf0.01"; // NOTE: Need to generate data in this dir first
  int numConcurrentUnits = 2;

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, numConcurrentUnits: {}",
			  dataDir, year, numConcurrentUnits);

  std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>> expected;
  auto expectedSQLite3Results = SQLite3::execute(
	  SQL::query1_1DateFilterSQLite(year, "temp"),
	  {filesystem::absolute(dataDir + "/date.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  auto mgr = Queries::query1_1DateFilterFilePullUp(dataDir,
												   year,
												   numConcurrentUnits);
  auto tupleSet = TestUtil::executeExecutionPlanTest(mgr);

  SPDLOG_DEBUG("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", tupleSet->numRows());

  	CHECK_EQ(expected->size(), tupleSet->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for lineorder scan and filter component of query 1.1
 *
 * Only checking row count at moment
 */
TEST_CASE ("ssb-benchmark-ep-query1_1-lineorderfilter-file-pullup" * doctest::skip(false || SKIP_SUITE)) {

  short discount = 2;
  short quantity = 25;
  std::string dataDir = "data/ssb-sf0.01"; // NOTE: Need to generate data in this dir first
  int numConcurrentUnits = 2;

  SPDLOG_INFO("Arguments  |  dataDir: '{}', discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, discount, quantity, numConcurrentUnits);

  std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>> expected;
  auto expectedSQLite3Results = SQLite3::execute(
	  SQL::query1_1LineOrderFilterSQLite(discount, quantity, "temp"),
	  {filesystem::absolute(dataDir + "/lineorder.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  auto mgr = Queries::query1_1LineOrderFilterFilePullUp(dataDir,
														discount, quantity,
														numConcurrentUnits);
  auto tupleSet = TestUtil::executeExecutionPlanTest(mgr);

  SPDLOG_DEBUG("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", tupleSet->numRows());

	  CHECK_EQ(expected->size(), tupleSet->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for join component of query 1.1
 *
 * Only checking row count at moment
 */
TEST_CASE ("ssb-benchmark-ep-query1_1-join-file-pullup" * doctest::skip(false || SKIP_SUITE)) {

  short year = 1992;
  short discount = 2;
  short quantity = 25;
  std::string dataDir = "data/ssb-sf0.01"; // NOTE: Need to generate data in this dir first
  int numConcurrentUnits = 2;

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>> expected;
  auto expectedSQLite3Results = SQLite3::execute(
	  SQL::query1_1JoinSQLite(year, discount, quantity, "temp"),
	  {filesystem::absolute(dataDir + "/date.tbl"),
	   filesystem::absolute(dataDir + "/lineorder.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  auto mgr = Queries::query1_1JoinFilePullUp(dataDir,
											 year, discount, quantity,
											 numConcurrentUnits);
  auto tupleSet = TestUtil::executeExecutionPlanTest(mgr);

  SPDLOG_DEBUG("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", tupleSet->numRows());

	  CHECK_EQ(expected->size(), tupleSet->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for query 1.1
 */
TEST_CASE ("ssb-benchmark-ep-query1_1-file-pullup" * doctest::skip(false || SKIP_SUITE)) {

  short year = 1992;
  short discount = 2;
  short quantity = 25;
  std::string dataDir = "data/ssb-sf0.01"; // NOTE: Need to generate data in this dir first
  int numConcurrentUnits = 2;

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>> expected;
  auto expectedSQLite3Results = SQLite3::execute(
	  SQL::query1_1SQLite(year, discount, quantity, "temp"),
	  {filesystem::absolute(dataDir + "/date.tbl"),
	   filesystem::absolute(dataDir + "/lineorder.tbl")});

  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  auto mgr = Queries::query1_1FilePullUp(dataDir,
										 year, discount, quantity,
										 numConcurrentUnits);
  auto tupleSet = TestUtil::executeExecutionPlanTest(mgr);

  SPDLOG_DEBUG("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expectedName = expectedSQLite3Results.value()->at(0).at(0).first;
  auto expectedValue = std::stod(expectedSQLite3Results.value()->at(0).at(0).second);

  auto actualName = tupleSet->getColumnByIndex(0).value()->getName();
  auto actualValue = tupleSet->getColumnByIndex(0).value()->element(0).value()->value<int>();

  SPDLOG_INFO("Expected  |  {} = {}", expectedName, expectedValue);
  SPDLOG_INFO("Actual  |  {} = {}", actualName, actualValue);

	  CHECK_EQ(expectedName, actualName);
	  CHECK_EQ(expectedValue, actualValue);
}

}
