//
// Created by matt on 24/4/20.
//
//
//#include <doctest/doctest.h>
//
//#include <normal/ssb/TestUtil.h>
//#include <normal/sql/Interpreter.h>
//#include <normal/connector/local-fs/LocalFileSystemConnector.h>
//#include <normal/connector/local-fs/LocalFileExplicitPartitioningScheme.h>
//#include <normal/connector/local-fs/LocalFileSystemCatalogueEntry.h>
//#include <normal/connector/s3/S3SelectConnector.h>
//#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
//#include <normal/connector/s3/S3SelectCatalogueEntry.h>
//#include <normal/pushdown/collate/Collate.h>
//#include <normal/tuple/TupleSet2.h>
//
//#include "normal/ssb/query1_1/SQL.h"
//#include <normal/plan/mode/Modes.h>
//#include <normal/cache/LRUCachingPolicy.h>
//
//using namespace normal::ssb;
//using namespace normal::ssb::query1_1;
//using namespace normal::pushdown;
//using namespace normal::tuple;
//
//void configureLocalConnector(normal::sql::Interpreter &i) {
//
//  auto conn = std::make_shared<normal::connector::local_fs::LocalFileSystemConnector>("local_fs");
//
//  auto cat = std::make_shared<normal::connector::Catalogue>("local_fs", conn);
//
//  auto partitioningScheme1 = std::make_shared<LocalFileExplicitPartitioningScheme>();
//  partitioningScheme1->add(std::make_shared<LocalFilePartition>("data/lineorder.csv"));
//  cat->put(std::make_shared<normal::connector::local_fs::LocalFileSystemCatalogueEntry>("lineorder",
//																						partitioningScheme1,
//																						cat));
//
//  auto partitioningScheme2 = std::make_shared<LocalFileExplicitPartitioningScheme>();
//  partitioningScheme2->add(std::make_shared<LocalFilePartition>("data/date.csv"));
//  cat->put(std::make_shared<normal::connector::local_fs::LocalFileSystemCatalogueEntry>("date",
//																						partitioningScheme2,
//																						cat));
//
//  i.put(cat);
//}
//
//void configureS3Connector(normal::sql::Interpreter &i) {
//  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
//  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);
//
//  auto partitioningScheme1 = std::make_shared<S3SelectExplicitPartitioningScheme>();
//  partitioningScheme1->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer.csv"));
//  cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>("customer", partitioningScheme1, cat));
//
//  // FIXME: Don't think these are the actual partitions, need to look them up
//  auto partitioningScheme2 = std::make_shared<S3SelectExplicitPartitioningScheme>();
//  partitioningScheme2->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer_01.csv"));
//  partitioningScheme2->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer_02.csv"));
//  partitioningScheme2->add(std::make_shared<S3SelectPartition>("s3Filter", "tpch-sf1/customer_03.csv"));
//  cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>("customer_partitioned",
//																		   partitioningScheme2,
//																		   cat));
//
//  i.put(cat);
//}
//
///**
// * This is out of date, commenting to stop compiler errors.
// * @param i
// * @return
// */
//auto execute(normal::sql::Interpreter& /* i */) {
////  i.getOperatorManager()->boot();
////  i.getOperatorManager()->start();
////  i.getOperatorManager()->join();
////
////  std::shared_ptr<normal::pushdown::Collate>
////	  collate = std::static_pointer_cast<normal::pushdown::Collate>(i.getOperatorManager()->getOperator("collate"));
////
////  auto tuples = collate->tuples();
////
////  SPDLOG_DEBUG("Output:\n{}", tuples->toString());
//
////  return tuples;
//	return nullptr;
//}
//
///**
// * This is out of date, commenting to stop compiler errors.
// * @param i
// * @return
// */
//std::shared_ptr<TupleSet2> executeSQLTest(const std::string& /* sql */) {
//
////  SPDLOG_INFO("SQL:\n{}", sql);
////
////  normal::sql::Interpreter i(normal::plan::operator_::mode::Modes::fullPushdownMode(), std::make_shared<LRUCachingPolicy>(100));
////
////  configureLocalConnector(i);
////  configureS3Connector(i);
////
////  i.parse(sql);
////
////  TestUtil::writeExecutionPlan(*i.getLogicalPlan());
////  TestUtil::writeExecutionPlan(*i.getOperatorManager());
////
////  auto tuples = execute(i);
////
////  i.getOperatorManager()->stop();
////
////  SPDLOG_INFO("Metrics:\n{}", i.getOperatorManager()->showMetrics());
////
////  auto tupleSet = TupleSet2::create(tuples);
////  return tupleSet;
//  return nullptr;
//}
//
//#define SKIP_SUITE true
//
//TEST_SUITE ("ssb-query1.1-sql" * doctest::skip(SKIP_SUITE)) {
//
//TEST_CASE ("ssb-benchmark-sql-query1_1" * doctest::skip(true || SKIP_SUITE)) {
//
//  short year = 1993;
//  short discount = 2;
//  short quantity = 24;
//
//  SPDLOG_INFO("Arguments  |  year: {}, discount: {}, quantity: {}",
//			  year, discount, quantity);
//
//  auto sql = SQL::full(year, discount, quantity, "local_fs");
//  auto tupleSet = executeSQLTest(sql);
//
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//}
//
//}
