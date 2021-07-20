//
// Created by matt on 5/3/20.
//

#include <experimental/filesystem>

#include <doctest/doctest.h>

#include <normal/sql/Interpreter.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3SelectCatalogueEntry.h>
#include <normal/connector/local-fs/LocalFileSystemConnector.h>
#include <normal/connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/connector/local-fs/LocalFileExplicitPartitioningScheme.h>
#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
#include "TestUtil.h"

using namespace normal::test;

void configureLocalConnector(normal::sql::Interpreter &i) {

  auto conn = std::make_shared<normal::connector::local_fs::LocalFileSystemConnector>("local_fs");

  auto cat = std::make_shared<normal::connector::Catalogue>("local_fs", conn);

  auto partitioningScheme1 = std::make_shared<LocalFileExplicitPartitioningScheme>();
  partitioningScheme1->add(std::make_shared<LocalFilePartition>("data/single-partition/test.csv"));
  cat->put(std::make_shared<normal::connector::local_fs::LocalFileSystemCatalogueEntry>("test",
																						partitioningScheme1,
																						cat));

  auto partitioningScheme2 = std::make_shared<LocalFileExplicitPartitioningScheme>();
  partitioningScheme2->add(std::make_shared<LocalFilePartition>("data/multi-partition/test01.csv"));
  partitioningScheme2->add(std::make_shared<LocalFilePartition>("data/multi-partition/test02.csv"));
  partitioningScheme2->add(std::make_shared<LocalFilePartition>("data/multi-partition/test03.csv"));
  cat->put(std::make_shared<normal::connector::local_fs::LocalFileSystemCatalogueEntry>("test_partitioned",
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

auto execute(normal::sql::Interpreter &/*i*/) {
//  i.getOperatorManager()->boot();
//  i.getOperatorManager()->start();
//  i.getOperatorManager()->join();
//
//  std::shared_ptr<normal::pushdown::Collate>
//	  collate = std::static_pointer_cast<normal::pushdown::Collate>(i.getOperatorManager()->getOperator("collate"));
//
//  auto tuples = collate->tuples();
//
//  SPDLOG_DEBUG("Output:\n{}", tuples->toString());
//
//  return tuples;
return nullptr;
}

auto executeTest(const std::string &/*sql*/) {

//  normal::sql::Interpreter i;
//
//  configureLocalConnector(i);
//  configureS3Connector(i);
//
//  i.parse(sql);
//
//  TestUtil::writeExecutionPlan(*i.getLogicalPlan());
//  TestUtil::writeExecutionPlan(*i.getOperatorManager());
//
//  auto tuples = execute(i);
//
//  i.getOperatorManager()->stop();
//
//  SPDLOG_INFO("Metrics:\n{}", i.getOperatorManager()->showMetrics());
//
//  return tuples;
return nullptr;
}

//TEST_CASE ("sql-select-sum_a-from-s3"
//               * doctest::skip(false)) {
//  auto tuples = executeTest("select * from s3_select.customer");
//
//}

TEST_CASE ("sql-select-sum_a-from-local" * doctest::skip(false)) {
//  auto tuples = executeTest("select sum(cast(A as double)), sum(cast(B as double)) from local_fs.test");
//	  CHECK(tuples->numRows() == 1);
//	  CHECK(tuples->numColumns() == 2);
//
//  // NOTE: Both columns have the same alias so need to access via column index
//	  CHECK(tuples->value<arrow::DoubleType>(0, 0) == 12.0);
//	  CHECK(tuples->value<arrow::DoubleType>(1, 0) == 15.0);
}

TEST_CASE ("sql-select-all-from-local" * doctest::skip(true)) {
//  auto tuples = executeTest("select * from local_fs.test");
//	  CHECK(tuples->numRows() == 3);
//	  CHECK(tuples->numColumns() == 3);
//
//  // NOTE: The arrow csv parser infers numeric strings to int64
//
//	  CHECK(tuples->value<arrow::Int64Type>("A", 0) == 1.0);
//	  CHECK(tuples->value<arrow::Int64Type>("A", 1) == 4.0);
//	  CHECK(tuples->value<arrow::Int64Type>("A", 2) == 7.0);
//	  CHECK(tuples->value<arrow::Int64Type>("B", 0) == 2.0);
//	  CHECK(tuples->value<arrow::Int64Type>("B", 1) == 5.0);
//	  CHECK(tuples->value<arrow::Int64Type>("B", 2) == 8.0);
//	  CHECK(tuples->value<arrow::Int64Type>("C", 0) == 3.0);
//	  CHECK(tuples->value<arrow::Int64Type>("C", 1) == 6.0);
//	  CHECK(tuples->value<arrow::Int64Type>("C", 2) == 9.0);
}

TEST_CASE ("sql-select-cast-from-local" * doctest::skip(true)) {
//  auto tuples = executeTest("select cast(A as double), cast(B as int) from local_fs.test");
//	  CHECK(tuples->numRows() == 3);
//	  CHECK(tuples->numColumns() == 2);
//	  CHECK(tuples->value<arrow::DoubleType>("A", 0) == 1.0);
//	  CHECK(tuples->value<arrow::DoubleType>("A", 1) == 4.0);
//	  CHECK(tuples->value<arrow::DoubleType>("A", 2) == 7.0);
//	  CHECK(tuples->value<arrow::Int32Type>("B", 0) == 2);
//	  CHECK(tuples->value<arrow::Int32Type>("B", 1) == 5);
//	  CHECK(tuples->value<arrow::Int32Type>("B", 2) == 8);
}

TEST_CASE ("sql-select-cast-from-local-multi-partition" * doctest::skip(true)) {
//  auto tuples = executeTest("select cast(A as double), cast(B as int) from local_fs.test_partitioned");
//	  CHECK(tuples->numRows() == 9);
//	  CHECK(tuples->numColumns() == 2);
//
//  auto columnA = tuples->vector<arrow::DoubleType>("A").value();
//  auto columnB = tuples->vector<arrow::Int32Type>("B").value();
//
//  /*
//   * NOTE: The multi-partition (i.e. parallel) executor will produce non-deterministic output, so we
//   * don't know exactly which row a value will be in. Need to sort before checking.
//   */
//  std::sort(columnA->begin(), columnA->end());
//  std::sort(columnB->begin(), columnB->end());
//
//	  CHECK(columnA->at(0) == 1.0);
//	  CHECK(columnA->at(1) == 2.0);
//	  CHECK(columnA->at(2) == 3.0);
//	  CHECK(columnA->at(3) == 4.0);
//	  CHECK(columnA->at(4) == 5.0);
//	  CHECK(columnA->at(5) == 6.0);
//	  CHECK(columnA->at(6) == 7.0);
//	  CHECK(columnA->at(7) == 8.0);
//	  CHECK(columnA->at(8) == 9.0);
//	  CHECK(columnB->at(0) == 11);
//	  CHECK(columnB->at(1) == 12);
//	  CHECK(columnB->at(2) == 13);
//	  CHECK(columnB->at(3) == 14);
//	  CHECK(columnB->at(4) == 15);
//	  CHECK(columnB->at(5) == 16);
//	  CHECK(columnB->at(6) == 17);
//	  CHECK(columnB->at(7) == 18);
//	  CHECK(columnB->at(8) == 19);
}
