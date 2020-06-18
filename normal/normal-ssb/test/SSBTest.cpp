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
  auto totalExecutionTime1 = mgr->getElapsedTime().value();
  SPDLOG_INFO("Metrics:\n{}", mgr->showMetrics());
    mgr->start();
    mgr->join();

    tuples = std::static_pointer_cast<Collate>(mgr->getOperator("collate"))->tuples();

    mgr->stop();
    auto totalExecutionTime2 = mgr->getElapsedTime().value();
    SPDLOG_INFO("Metrics:\n{}", mgr->showMetrics());
    SPDLOG_INFO("Execute for the first and second time:{},{}\n", totalExecutionTime1,totalExecutionTime2);

  auto tupleSet = TupleSet2::create(tuples);
  return tupleSet;
}

#define SKIP_SUITE false

TEST_SUITE ("ssb" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-benchmark-sql-query1_1" * doctest::skip(true || SKIP_SUITE)) {

  short year = 1993;
  short discount = 2;
  short quantity = 24;

  SPDLOG_INFO("Arguments  |  year: {}, discount: {}, quantity: {}",
			  year, discount, quantity);

  auto sql = Queries::query01(year, discount, quantity);
  auto tupleSet = executeSQLTest(sql);

  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("ssb-benchmark-ep-query1_1-file-pullup" * doctest::skip(true || SKIP_SUITE)) {

  short year = 1993;
  short discount = 2;
  short quantity = 24;
  std::string dataDir = "data/ssb-sf1"; // NOTE: Need to generate data in this dir first

  SPDLOG_INFO("Arguments  |  dataDir: '{}', year: {}, discount: {}, quantity: {}",
			  dataDir, year, discount, quantity);

  auto mgr = Queries::query1_1FilePullUp(dataDir,
										 year, discount, quantity);
  auto tupleSet = executeExecutionPlanTest(mgr);

  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("ssb-benchmark-ep-query1_1-file-pullup-parallel" * doctest::skip(true || SKIP_SUITE)) {

  short year = 1993;
  short discount = 2;
  short quantity = 24;
  std::string dataDir = "data/ssb-sf1"; // NOTE: Need to generate data in this dir first
  short numPartitions = 2;

  SPDLOG_INFO("Arguments  |  dataDir: '{}', numPartitions: {}, year: {}, discount: {}, quantity: {}",
			  dataDir, numPartitions, year, discount, quantity);

  auto mgr = Queries::query1_1FilePullUpParallel(dataDir,
												 year, discount, quantity,
												 numPartitions);
  auto tupleSet = executeExecutionPlanTest(mgr);

  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 100)));
}

TEST_CASE ("ssb-benchmark-ep-query1_1-s3-pullup" * doctest::skip(true || SKIP_SUITE)) {

  short year = 1993;
  short discount = 2;
  short quantity = 24;
  std::string s3Bucket = "s3filter";
  std::string s3ObjectDir = "ssb-sf1";

  SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', year: {}, discount: {}, quantity: {}",
			  s3Bucket, s3ObjectDir, year, discount, quantity);

  AWSClient client;
  client.init();
  auto mgr = Queries::query1_1S3PullUp(s3Bucket, s3ObjectDir,
									   year, discount, quantity,
									   client);
  auto tupleSet = executeExecutionPlanTest(mgr);

  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("ssb-benchmark-ep-query1_1-s3-pullup-parallel" * doctest::skip(false || SKIP_SUITE)) {

  short year = 1992;
  short discount = 2;
  short quantity = 24;
  std::string s3Bucket = "s3filter";
  std::string s3ObjectDir = "ssb-sf1";
  short numPartitions = 16;

  SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', numPartitions: {}, year: {}, discount: {}, quantity: {}",
			  s3Bucket, s3ObjectDir, numPartitions, year, discount, quantity);

  AWSClient client;
  client.init();
  auto mgr = Queries::query1_1S3PullUpParallel(s3Bucket, s3ObjectDir,
											   year, discount, quantity,
											   numPartitions, client);
  auto tupleSet = executeExecutionPlanTest(mgr);

  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("ssb-benchmark-ep-query1_1-s3-pushdown" * doctest::skip(true || SKIP_SUITE)) {

  short year = 1992;
  short discount = 2;
  short quantity = 24;
  std::string s3Bucket = "s3filter";
  std::string s3ObjectDir = "ssb-sf1";

  SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', year: {}, discount: {}, quantity: {}",
			  s3Bucket, s3ObjectDir, year, discount, quantity);

  AWSClient client;
  client.init();
  auto mgr = Queries::query1_1S3PushDown(s3Bucket, s3ObjectDir,
										 year, discount, quantity,
										 client);
  auto tupleSet = executeExecutionPlanTest(mgr);

  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("ssb-benchmark-ep-query1_1-s3-pushdown-parallel" * doctest::skip(true || SKIP_SUITE)) {

  short year = 1992;
  short discount = 2;
  short quantity = 24;
  std::string s3Bucket = "s3filter";
  std::string s3ObjectDir = "ssb-sf1";
  short numPartitions = 16;

  SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', numPartitions: {}, year: {}, discount: {}, quantity: {}",
			  s3Bucket, s3ObjectDir, numPartitions, year, discount, quantity);

  AWSClient client;
  client.init();
  auto mgr = Queries::query1_1S3PushDownParallel(s3Bucket, s3ObjectDir,
												 year, discount, quantity,
												 numPartitions, client);
  auto tupleSet = executeExecutionPlanTest(mgr);

  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

}
