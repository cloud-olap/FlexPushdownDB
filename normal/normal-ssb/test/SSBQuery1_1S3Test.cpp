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

#include "normal/ssb/query1_1/S3SelectQueries.h"
#include "normal/ssb/query1_1/SQL.h"

using namespace normal::ssb;
using namespace normal::ssb::query1_1;
using namespace normal::pushdown;
using namespace normal::tuple;


#define SKIP_SUITE true

TEST_SUITE ("ssb-query1.1-s3" * doctest::skip(SKIP_SUITE)) {

/**
 * Tests that SQLLite and Normal produce the same output for lineorder scan component of query 1.1 against s3
 * using pull up strategy
 *
 * Only checking row count at moment
 */
TEST_CASE ("ssb-benchmark-ep-query1_1-lineorderscan-s3-pullup" * doctest::skip(false || SKIP_SUITE)) {

  std::string dataDir = "data/ssb-sf0.01"; // NOTE: Need to generate data in this dir first
  std::string s3Bucket = "s3filter";
  std::string s3ObjectDir = "ssb-sf0.01";
  int numConcurrentUnits = 1;

  SPDLOG_INFO("Arguments  |  dataDir: '{}', numConcurrentUnits: {}",
			  dataDir, numConcurrentUnits);

  std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>> expected;
  auto expectedSQLite3Results = SQLite3::execute(
	  SQL::lineOrderScan("temp"),
	  {filesystem::absolute(dataDir + "/lineorder.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  AWSClient client;
  client.init();
  auto mgr = S3SelectQueries::lineOrderScanPullUp(s3Bucket, s3ObjectDir,
												  numConcurrentUnits,
												  client);
  auto tupleSet = TestUtil::executeExecutionPlanTest(mgr);

  SPDLOG_DEBUG("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", tupleSet->numRows());

	  CHECK_EQ(expected->size(), tupleSet->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for date scan and filter component of query 1.1
 * running against s3 using pull up strategy
 *
 * Only checking row count at moment
 */
TEST_CASE ("ssb-benchmark-ep-query1_1-datefilter-s3-pullup" * doctest::skip(false || SKIP_SUITE)) {

  short year = 1992;
  std::string dataDir = "data/ssb-sf0.01"; // NOTE: Need to generate data in this dir first
  std::string s3Bucket = "s3filter";
  std::string s3ObjectDir = "ssb-sf0.01";
  int numConcurrentUnits = 2;

  SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', year: {}, numConcurrentUnits: {}",
			  s3Bucket, s3ObjectDir, year, numConcurrentUnits);

  std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>> expected;
  auto expectedSQLite3Results = SQLite3::execute(
	  SQL::dateFilter(year, "temp"),
	  {filesystem::absolute(dataDir + "/date.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  AWSClient client;
  client.init();
  auto mgr = S3SelectQueries::dateFilterPullUp(s3Bucket, s3ObjectDir,
											   year,
											   numConcurrentUnits,
											   client);
  auto tupleSet = TestUtil::executeExecutionPlanTest(mgr);

  SPDLOG_DEBUG("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", tupleSet->numRows());

	  CHECK_EQ(expected->size(), tupleSet->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for lineorder scan and filter component of query 1.1
 * running against s3 using pull up strategy
 *
 * Only checking row count at moment
 */
TEST_CASE ("ssb-benchmark-ep-query1_1-lineorderfilter-s3-pullup" * doctest::skip(false || SKIP_SUITE)) {

  short discount = 2;
  short quantity = 25;
  std::string dataDir = "data/ssb-sf0.01"; // NOTE: Need to generate data in this dir first
  std::string s3Bucket = "s3filter";
  std::string s3ObjectDir = "ssb-sf0.01";
  int numConcurrentUnits = 1;

  SPDLOG_INFO("Arguments  |  dataDir: '{}', discount: {}, quantity: {}, numConcurrentUnits: {}",
			  dataDir, discount, quantity, numConcurrentUnits);

  std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>> expected;
  auto expectedSQLite3Results = SQLite3::execute(
	  SQL::lineOrderFilter(discount, quantity, "temp"),
	  {filesystem::absolute(dataDir + "/lineorder.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  AWSClient client;
  client.init();
  auto mgr = S3SelectQueries::lineOrderFilterPullUp(s3Bucket, s3ObjectDir,
													discount, quantity,
													numConcurrentUnits,
													client);
  auto tupleSet = TestUtil::executeExecutionPlanTest(mgr);

  SPDLOG_DEBUG("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", tupleSet->numRows());

	  CHECK_EQ(expected->size(), tupleSet->numRows());
}

/**
 * Tests that SQLLite and Normal produce the same output for query 1.1 running against s3 using pull up strategy
 */
//TEST_CASE ("ssb-benchmark-ep-query1_1-s3-pullup" * doctest::skip(false || SKIP_SUITE)) {
//
//  short year = 1992;
//  short discount = 2;
//  short quantity = 25;
//  std::string s3Bucket = "s3filter";
//  std::string s3ObjectDir = "ssb-sf0.01";
//  int numConcurrentUnits = 1;
//
//  SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', year: {}, discount: {}, quantity: {}, numConcurrentUnits: {}",
//			  s3Bucket, s3ObjectDir, year, discount, quantity, numConcurrentUnits);
//
//  AWSClient client;
//  client.init();
//  auto mgr = S3SelectQueries::query1_1S3PullUp(s3Bucket, s3ObjectDir,
//									   year, discount, quantity,
//									   numConcurrentUnits,
//									   client);
//  auto tupleSet = executeExecutionPlanTest(mgr);
//
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//
//  auto actualName = tupleSet->getColumnByIndex(0).value()->getName();
//  auto actualValue = tupleSet->getColumnByIndex(0).value()->element(0).value()->value<int>();
//
//  SPDLOG_INFO("Actual  |  {} = {}", actualName, actualValue);
//}

//TEST_CASE ("ssb-benchmark-ep-query1_1-s3-pullup-parallel" * doctest::skip(true || SKIP_SUITE)) {
//
//  short year = 1992;
//  short discount = 2;
//  short quantity = 24;
//  std::string s3Bucket = "s3filter";
//  std::string s3ObjectDir = "ssb-sf0.01";
//  short numPartitions = 16;
//
//  SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', numPartitions: {}, year: {}, discount: {}, quantity: {}",
//			  s3Bucket, s3ObjectDir, numPartitions, year, discount, quantity);
//
//  AWSClient client;
//  client.init();
//  auto mgr = S3SelectQueries::query1_1S3PullUpParallel(s3Bucket, s3ObjectDir,
//											   year, discount, quantity,
//											   numPartitions, client);
//  auto tupleSet = executeExecutionPlanTest(mgr);
//
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//}
//
//TEST_CASE ("ssb-benchmark-ep-query1_1-s3-pushdown" * doctest::skip(true || SKIP_SUITE)) {
//
//  short year = 1992;
//  short discount = 2;
//  short quantity = 24;
//  std::string s3Bucket = "s3filter";
//  std::string s3ObjectDir = "ssb-sf1";
//
//  SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', year: {}, discount: {}, quantity: {}",
//			  s3Bucket, s3ObjectDir, year, discount, quantity);
//
//  AWSClient client;
//  client.init();
//  auto mgr = S3SelectQueries::query1_1S3PushDown(s3Bucket, s3ObjectDir,
//										 year, discount, quantity,
//										 client);
//  auto tupleSet = executeExecutionPlanTest(mgr);
//
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//}

//TEST_CASE ("ssb-benchmark-ep-query1_1-s3-pushdown-parallel" * doctest::skip(true || SKIP_SUITE)) {
//
//  short year = 1992;
//  short discount = 2;
//  short quantity = 24;
//  std::string s3Bucket = "s3filter";
//  std::string s3ObjectDir = "ssb-sf1";
//  short numPartitions = 16;
//
//  SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', numPartitions: {}, year: {}, discount: {}, quantity: {}",
//			  s3Bucket, s3ObjectDir, numPartitions, year, discount, quantity);
//
//  AWSClient client;
//  client.init();
//  auto mgr = S3SelectQueries::query1_1S3PushDownParallel(s3Bucket, s3ObjectDir,
//												 year, discount, quantity,
//												 numPartitions, client);
//  auto tupleSet = executeExecutionPlanTest(mgr);
//
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//}
//
//TEST_CASE ("ssb-benchmark-ep-query1_1-s3-hyrbid-parallel" * doctest::skip(true || SKIP_SUITE)) {
//
//  short year = 1992;
//  short discount = 2;
//  short quantity = 24;
//  std::string s3Bucket = "s3filter";
//  std::string s3ObjectDir = "ssb-sf1";
//  short numPartitions = 16;
//
//  SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', numPartitions: {}, year: {}, discount: {}, quantity: {}",
//			  s3Bucket, s3ObjectDir, numPartitions, year, discount, quantity);
//
//  AWSClient client;
//  client.init();
//  auto mgr = S3SelectQueries::query1_1S3HybridParallel(s3Bucket, s3ObjectDir,
//											   year, discount, quantity,
//											   numPartitions, client);
//  auto tupleSet = executeExecutionPlanTest(mgr);
//
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//}

}
