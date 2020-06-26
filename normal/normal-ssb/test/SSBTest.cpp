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

#include "normal/ssb/Queries.h"

using namespace normal::ssb;
using namespace normal::pushdown;
using namespace normal::tuple;

void configureLocalConnector(normal::sql::Interpreter &i) {

  auto conn = std::make_shared<normal::connector::local_fs::LocalFileSystemConnector>("local_fs");

  auto cat = std::make_shared<normal::connector::Catalogue>("local_fs", conn);

  auto partitioningScheme1 = std::make_shared<LocalFileExplicitPartitioningScheme>();
  partitioningScheme1->add(std::make_shared<LocalFilePartition>("data/lineorder.csv"));
  cat->put(std::make_shared<normal::connector::local_fs::LocalFileSystemCatalogueEntry>("lineorder",
																						partitioningScheme1,
																						cat));

  auto partitioningScheme2 = std::make_shared<LocalFileExplicitPartitioningScheme>();
  partitioningScheme2->add(std::make_shared<LocalFilePartition>("data/date.csv"));
  cat->put(std::make_shared<normal::connector::local_fs::LocalFileSystemCatalogueEntry>("date",
																						partitioningScheme2,
																						cat));

  i.put(cat);
}

void configureS3Connector(normal::sql::Interpreter &i) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  auto partitioningScheme1 = std::make_shared<S3SelectExplicitPartitioningScheme>();
  partitioningScheme1->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer.csv"));
  cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>("customer", partitioningScheme1, cat));

  // FIXME: Don't think these are the actual partitions, need to look them up
  auto partitioningScheme2 = std::make_shared<S3SelectExplicitPartitioningScheme>();
  partitioningScheme2->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer_01.csv"));
  partitioningScheme2->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer_02.csv"));
  partitioningScheme2->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer_03.csv"));
  cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>("customer_partitioned",
																		   partitioningScheme2,
																		   cat));

  i.put(cat);
}

auto execute(normal::sql::Interpreter &i) {
  i.getOperatorManager()->boot();
  i.getOperatorManager()->start();
  i.getOperatorManager()->join();

  std::shared_ptr<normal::pushdown::Collate>
	  collate = std::static_pointer_cast<normal::pushdown::Collate>(i.getOperatorManager()->getOperator("collate"));

  auto tuples = collate->tuples();

  SPDLOG_DEBUG("Output:\n{}", tuples->toString());

  return tuples;
}

std::shared_ptr<TupleSet2> executeSQLTest(const std::string &sql) {

  SPDLOG_INFO("SQL:\n{}", sql);

  normal::sql::Interpreter i;

  configureLocalConnector(i);
  configureS3Connector(i);

  i.parse(sql);

  TestUtil::writeExecutionPlan(*i.getLogicalPlan());
  TestUtil::writeExecutionPlan(*i.getOperatorManager());

  auto tuples = execute(i);

  i.getOperatorManager()->stop();

  SPDLOG_INFO("Metrics:\n{}", i.getOperatorManager()->showMetrics());

  auto tupleSet = TupleSet2::create(tuples);
  return tupleSet;
}

std::shared_ptr<TupleSet2> executeExecutionPlanTest(const std::shared_ptr<OperatorManager> &mgr) {

  TestUtil::writeExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = std::static_pointer_cast<Collate>(mgr->getOperator("collate"))->tuples();

  mgr->stop();
//  auto totalExecutionTime1 = mgr->getElapsedTime().value();
  SPDLOG_INFO("Metrics:\n{}", mgr->showMetrics());
//  mgr->start();
//  mgr->join();
//
//  tuples = std::static_pointer_cast<Collate>(mgr->getOperator("collate"))->tuples();
//
//  mgr->stop();
//  auto totalExecutionTime2 = mgr->getElapsedTime().value();
//  SPDLOG_INFO("Metrics:\n{}", mgr->showMetrics());
//  SPDLOG_INFO("Execute for the first and second time:{},{}\n", totalExecutionTime1, totalExecutionTime2);

  auto tupleSet = TupleSet2::create(tuples);
  return tupleSet;
}

#define SKIP_SUITE false

TEST_SUITE ("ssb" * doctest::skip(SKIP_SUITE)) {

//TEST_CASE ("ssb-benchmark-sql-query1_1" * doctest::skip(true || SKIP_SUITE)) {
//
//  short year = 1993;
//  short discount = 2;
//  short quantity = 24;
//
//  SPDLOG_INFO("Arguments  |  year: {}, discount: {}, quantity: {}",
//			  year, discount, quantity);
//
//  auto sql = Queries::query01(year, discount, quantity, "local_fs");
//  auto tupleSet = executeSQLTest(sql);
//
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//}

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
	  Queries::query1_1DateFilterSQLite(year, "temp"),
	  {filesystem::absolute(dataDir + "/date.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  auto mgr = Queries::query1_1DateFilterFilePullUp(dataDir,
												   year,
												   numConcurrentUnits);
  auto tupleSet = executeExecutionPlanTest(mgr);

  SPDLOG_DEBUG("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  SPDLOG_INFO("Expected  |  numRows: {}", expected->size());
  SPDLOG_INFO("Actual  |  numRows: {}", tupleSet->numRows());

  	CHECK_EQ(expected->size(), tupleSet->numRows());
}

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
	  Queries::query1_1LineOrderScanSQLite("temp"),
	  {filesystem::absolute(dataDir + "/lineorder.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  AWSClient client;
  client.init();
  auto mgr = Queries::query1_1LineOrderScanS3PullUp(s3Bucket, s3ObjectDir,
													  numConcurrentUnits,
													  client);
  auto tupleSet = executeExecutionPlanTest(mgr);

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
	  Queries::query1_1DateFilterSQLite(year, "temp"),
	  {filesystem::absolute(dataDir + "/date.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  AWSClient client;
  client.init();
  auto mgr = Queries::query1_1DateFilterS3PullUp(s3Bucket, s3ObjectDir,
												 year,
												 numConcurrentUnits,
												 client);
  auto tupleSet = executeExecutionPlanTest(mgr);

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
	  Queries::query1_1LineOrderFilterSQLite(discount, quantity, "temp"),
	  {filesystem::absolute(dataDir + "/lineorder.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  AWSClient client;
  client.init();
  auto mgr = Queries::query1_1LineOrderFilterS3PullUp(s3Bucket, s3ObjectDir,
													  discount, quantity,
													  numConcurrentUnits,
													  client);
  auto tupleSet = executeExecutionPlanTest(mgr);

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
	  Queries::query1_1LineOrderFilterSQLite(discount, quantity, "temp"),
	  {filesystem::absolute(dataDir + "/lineorder.tbl")});
  if (!expectedSQLite3Results.has_value()) {
		FAIL(fmt::format("Error: {}", expectedSQLite3Results.error()));
  } else {
	expected = expectedSQLite3Results.value();
  }

  auto mgr = Queries::query1_1LineOrderFilterFilePullUp(dataDir,
														discount, quantity,
														numConcurrentUnits);
  auto tupleSet = executeExecutionPlanTest(mgr);

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
	  Queries::query1_1JoinSQLite(year, discount, quantity, "temp"),
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
  auto tupleSet = executeExecutionPlanTest(mgr);

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
	  Queries::query1_1SQLite(year, discount, quantity, "temp"),
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
  auto tupleSet = executeExecutionPlanTest(mgr);

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
//  auto mgr = Queries::query1_1S3PullUp(s3Bucket, s3ObjectDir,
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
//  auto mgr = Queries::query1_1S3PullUpParallel(s3Bucket, s3ObjectDir,
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
//  auto mgr = Queries::query1_1S3PushDown(s3Bucket, s3ObjectDir,
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
//  auto mgr = Queries::query1_1S3PushDownParallel(s3Bucket, s3ObjectDir,
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
//  auto mgr = Queries::query1_1S3HybridParallel(s3Bucket, s3ObjectDir,
//											   year, discount, quantity,
//											   numPartitions, client);
//  auto tupleSet = executeExecutionPlanTest(mgr);
//
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//}

}
