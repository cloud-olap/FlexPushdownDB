//
// Created by matt on 24/4/20.
//

#include <vector>

#include <doctest/doctest.h>

#include <normal/test/TestUtil.h>
#include <normal/sql/Interpreter.h>
#include <normal/connector/local-fs/LocalFileSystemConnector.h>
#include <normal/connector/local-fs/LocalFileExplicitPartitioningScheme.h>
#include <normal/connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
#include <normal/connector/s3/S3SelectCatalogueEntry.h>
#include <normal/pushdown/Collate.h>

#include "normal/ssb/Queries.h"

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

auto executeTest(const std::string &sql) {

  SPDLOG_INFO("SQL:\n{}", sql);

  normal::sql::Interpreter i;

  configureLocalConnector(i);
  configureS3Connector(i);

  i.parse(sql);

  normal::test::TestUtil::writeExecutionPlan(i);

  auto tuples = execute(i);

  i.getOperatorManager()->stop();

  SPDLOG_INFO("Metrics:\n{}", i.getOperatorManager()->showMetrics());

  return tuples;
}

TEST_CASE ("ssb-benchmark-query01") {

  short year = 1993;
  short discount = 2;
  short quantity = 24;

  auto sql = Queries::query01(year, discount, quantity);

  auto tuples = executeTest(sql);
}
