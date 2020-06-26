//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>

#include <normal/ssb/TestUtil.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/ssb/SQLite3.h>
#include <normal/ssb/query1_1/LocalFileSystemQueries.h>
#include <normal/ssb/query1_1/SQL.h>

using namespace normal::ssb;
using namespace normal::ssb::query1_1;
using namespace normal::pushdown;
using namespace normal::tuple;

/**
 * Runs the given query in sql lite, returning the results or failing the test on an error
 */
std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>>
executeSQLite(std::string sql, std::vector<std::string> dataFiles) {

  std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>> expected;
  auto expectedSQLite3Results = SQLite3::execute(sql, dataFiles);
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  return expected;
}

/**
 * Runs the given Normal execution plan, returning the results or failing the test on an error
 */
std::shared_ptr<TupleSet2> executeExecutionPlan(std::shared_ptr<OperatorManager> mgr) {
  auto tupleSet = TestUtil::executeExecutionPlanTest(mgr);
  SPDLOG_DEBUG("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  return tupleSet;
}

/**
 * Tests that SQLLite and Normal produce the same output for date scan component of query 1.1
 *
 * Only checking row count at moment
 */
void dateScan(std::string dataDir, int numConcurrentUnits) {

  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, numConcurrentUnits);

  auto expected = executeSQLite(SQL::query1_1DateScanSQLite("temp"),
								{filesystem::absolute(dataDir + "/date.tbl")});

  auto actual = executeExecutionPlan(LocalFileSystemQueries::dateScanQuery(dataDir,
																			 numConcurrentUnits));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

	  CHECK_EQ(expected->size(), actual->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for date scan and filter component of query 1.1
 *
 * Only checking row count at moment
 */
void dateFilter(short year, std::string dataDir, int numConcurrentUnits) {

  SPDLOG_INFO("Arguments  |  year: {}, dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, year, numConcurrentUnits);

  auto expected = executeSQLite(SQL::query1_1DateFilterSQLite(year, "temp"),
								{filesystem::absolute(dataDir + "/date.tbl")});

  auto actual = executeExecutionPlan(LocalFileSystemQueries::dateFilterQuery(dataDir,
																			 year,
																			 numConcurrentUnits));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

	  CHECK_EQ(expected->size(), actual->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for lineorder scan component of query 1.1
 *
 * Only checking row count at moment
 */
void lineOrderScan(std::string dataDir, int numConcurrentUnits) {
  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, numConcurrentUnits);

  auto expected = executeSQLite(SQL::query1_1LineOrderScanSQLite("temp"),
								{filesystem::absolute(dataDir + "/lineorder.tbl")});

  auto actual = executeExecutionPlan(LocalFileSystemQueries::lineOrderScanQuery(dataDir,
																				numConcurrentUnits));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

	  CHECK_EQ(expected->size(), actual->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for lineorder scan and filter component of query 1.1
 *
 * Only checking row count at moment
 */
void lineOrderFilter(short discount, short quantity, std::string dataDir, int numConcurrentUnits) {
  SPDLOG_INFO("Arguments  |  dataDir: '{}', discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, discount, quantity, numConcurrentUnits);

  auto expected = executeSQLite(SQL::query1_1LineOrderFilterSQLite(discount, quantity, "temp"),
								{filesystem::absolute(dataDir + "/lineorder.tbl")});

  auto actual = executeExecutionPlan(LocalFileSystemQueries::lineOrderFilterQuery(dataDir,
																				  discount, quantity,
																				  numConcurrentUnits));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

	  CHECK_EQ(expected->size(), actual->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for join component of query 1.1
 *
 * Only checking row count at moment
 */
void join(short year, short discount, short quantity, std::string dataDir, int numConcurrentUnits) {
  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  auto expected = executeSQLite(SQL::query1_1JoinSQLite(year, discount, quantity, "temp"),
								{filesystem::absolute(dataDir + "/date.tbl"),
								 filesystem::absolute(dataDir + "/lineorder.tbl")});

  auto actual = executeExecutionPlan(LocalFileSystemQueries::joinQuery(dataDir,
																	   year, discount, quantity,
																	   numConcurrentUnits));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", actual->numRows());

	  CHECK_EQ(expected->size(), actual->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for full query 1.1
 */
void aggregate(short year, short discount, short quantity, std::string dataDir, int numConcurrentUnits) {
  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, year, discount, quantity, numConcurrentUnits);

  auto expected = executeSQLite(SQL::query1_1SQLite(year, discount, quantity, "temp"),
								{filesystem::absolute(dataDir + "/date.tbl"),
								 filesystem::absolute(dataDir + "/lineorder.tbl")});

  auto actual = executeExecutionPlan(LocalFileSystemQueries::fullQuery(dataDir,
																	   year, discount, quantity,
																	   numConcurrentUnits));

  auto expectedName = expected->at(0).at(0).first;
  auto expectedValue = std::stod(expected->at(0).at(0).second);

  auto actualName = actual->getColumnByIndex(0).value()->getName();
  auto actualValue = actual->getColumnByIndex(0).value()->element(0).value()->value<int>();

  SPDLOG_INFO("Expected  |  {} = {}", expectedName, expectedValue);
  SPDLOG_INFO("Actual  |  {} = {}", actualName, actualValue);

	  CHECK_EQ(expectedName, actualName);
	  CHECK_EQ(expectedValue, actualValue);
}


#define SKIP_SUITE false

TEST_SUITE ("ssb-benchmark-query1.1-file" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("date-scan" * doctest::skip(false || SKIP_SUITE)) {
  dateScan("data/ssb-sf0.01", 1);
  dateScan("data/ssb-sf0.01", 2);
  dateScan("data/ssb-sf1", 1);
  dateScan("data/ssb-sf1", 2);
}

TEST_CASE ("lineorder-scan" * doctest::skip(false || SKIP_SUITE)) {
  lineOrderScan("data/ssb-sf0.01", 1);
  lineOrderScan("data/ssb-sf0.01", 2);
  lineOrderScan("data/ssb-sf1", 1);
  lineOrderScan("data/ssb-sf1", 2);
}

TEST_CASE ("date-filter" * doctest::skip(false || SKIP_SUITE)) {
  dateFilter(1992, "data/ssb-sf0.01", 1);
  dateFilter(1992, "data/ssb-sf0.01", 2);
  dateFilter(1992, "data/ssb-sf1", 1);
  dateFilter(1992, "data/ssb-sf1", 2);
}

TEST_CASE ("lineorder-filter" * doctest::skip(false || SKIP_SUITE)) {
  lineOrderFilter(2, 25, "data/ssb-sf0.01", 1);
  lineOrderFilter(2, 25, "data/ssb-sf0.01", 2);
  lineOrderFilter(2, 25, "data/ssb-sf1", 1);
  lineOrderFilter(2, 25, "data/ssb-sf1", 2);
}

TEST_CASE ("join" * doctest::skip(false || SKIP_SUITE)) {
  ::join(1992, 2, 25, "data/ssb-sf0.01", 1);
  ::join(1992, 2, 25, "data/ssb-sf0.01", 2);
  ::join(1992, 2, 25, "data/ssb-sf1", 1);
  ::join(1992, 2, 25, "data/ssb-sf1", 2);
}

TEST_CASE ("aggregate" * doctest::skip(false || SKIP_SUITE)) {
  ::aggregate(1992, 2, 25, "data/ssb-sf0.01", 1);
  ::aggregate(1992, 2, 25, "data/ssb-sf0.01", 2);
  ::aggregate(1992, 2, 25, "data/ssb-sf1", 1);
  ::aggregate(1992, 2, 25, "data/ssb-sf1", 2);
}

}
