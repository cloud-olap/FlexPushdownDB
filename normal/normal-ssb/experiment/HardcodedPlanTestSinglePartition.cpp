//
// Created by Yifei Yang on 7/20/20.
//

#include <doctest/doctest.h>
#include <normal/ssb/TestUtil.h>
#include <normal/pushdown/Collate.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <normal/pushdown/shuffle/Shuffle.h>
#include <normal/pushdown/aggregate/AggregationFunction.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/pushdown/Util.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/Project.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/pushdown/group/Group.h>
#include <normal/plan/Globals.h>

#define SKIP_SUITE false

using namespace normal::ssb;
using namespace normal::pushdown;
using namespace normal::expression::gandiva;

TEST_SUITE ("Hardcoded-plan-test-single-partition" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("SimpleScan" * doctest::skip(true || SKIP_SUITE)) {
//  normal::pushdown::AWSClient client;
//  client.init();
//
//  // operators
//  auto s3Bucket = "pushdowndb";
//  auto s3Object = "ssb-sf0.01/csv/date.tbl";
//  std::vector<std::string> s3Objects = {s3Object};
//  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
//  auto numBytes = partitionMap.find(s3Object)->second;
//  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  std::vector<std::string> columns = {"D_DATEKEY"};
//  auto lineorderScan = normal::pushdown::S3SelectScan::make(
//          "SimpleScan",
//          "pushdowndb",
//          s3Object,
//          fmt::format(""),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          normal::plan::DefaultS3Client,
//          true);
//
//  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);
//
//  // wire up
//  auto mgr = std::make_shared<OperatorManager>();
//  lineorderScan->produce(collate);
//  collate->consume(lineorderScan);
//  mgr->put(lineorderScan);
//  mgr->put(collate);
//
//  // execute
//  mgr->boot();
//  mgr->start();
//  mgr->join();
//  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(mgr->getOperator("collate"))->tuples();
//  mgr->stop();
//
//  auto tupleSet = TupleSet2::create(tuples);
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  SPDLOG_INFO("Metrics:\n{}", mgr->showMetrics());
//  SPDLOG_INFO("Finish");
}

TEST_CASE ("Join_Two" * doctest::skip(true || SKIP_SUITE)) {
//  normal::pushdown::AWSClient client;
//  client.init();
//
//  // operators
//  auto s3Bucket = "s3filter";
//  std::vector<std::string> s3Objects = {"ssb-sf0.01/part.tbl", "ssb-sf0.01/lineorder.tbl"};
//  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
//
//  // lineorder scan
//  auto numBytes = partitionMap.find("ssb-sf0.01/lineorder.tbl")->second;
//  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  std::vector<std::string> columns = {"lo_orderkey", "lo_partkey", "lo_extendedprice", "lo_discount"};
//  auto lineorderScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/lineorder.tbl",
//          "s3filter",
//          "ssb-sf0.01/lineorder.tbl",
//          fmt::format("select lo_orderkey, lo_partkey, lo_extendedprice, lo_discount from s3Object "
//                      "where cast(lo_discount as int) between 1 and 3"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // part scan
//  numBytes = partitionMap.find("ssb-sf0.01/part.tbl")->second;
//  scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  columns = {"p_partkey", "p_name"};
//  auto partScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/part.tbl",
//          "s3filter",
//          "ssb-sf0.01/part.tbl",
//          fmt::format("select p_partkey, p_name from s3Object where (p_mfgr = 'MFGR#1' and p_mfgr = 'MFGR#2')"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // shuffle
//  auto partShuffle = normal::pushdown::shuffle::Shuffle::make("partShuffle", "p_partkey");
//  auto lineorderShuffle = normal::pushdown::shuffle::Shuffle::make("lineorderShuffle", "lo_partkey");
//
//  // join
//  auto joinBuild = std::make_shared<normal::pushdown::join::HashJoinBuild>("join-build", "lo_partkey");
//  auto joinProbe = std::make_shared<normal::pushdown::join::HashJoinProbe>("join-probe-{}",
//                                                                           normal::pushdown::join::JoinPredicate::create("lo_partkey","p_partkey"));
//
//  // collate
//  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);
//
//  // wire up
//  auto mgr = std::make_shared<OperatorManager>();
//
//  partScan->produce(partShuffle);
//  partShuffle->consume(partScan);
//
//  lineorderScan->produce(lineorderShuffle);
//  lineorderShuffle->produce(lineorderScan);
//
//  lineorderShuffle->produce(joinBuild);
//  joinBuild->consume(lineorderShuffle);
//
//  partShuffle->produce(joinProbe);
//  joinProbe->consume(partShuffle);
//
//  joinBuild->produce(joinProbe);
//  joinProbe->consume(joinBuild);
//
//  joinProbe->produce(collate);
//  collate->consume(joinProbe);
//
//  mgr->put(partScan);
//  mgr->put(lineorderScan);
//  mgr->put(partShuffle);
//  mgr->put(lineorderShuffle);
//  mgr->put(joinBuild);
//  mgr->put(joinProbe);
//  mgr->put(collate);
//
//  // execute
//  mgr->boot();
//  mgr->start();
//  mgr->join();
//  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(mgr->getOperator("collate"))->tuples();
//  mgr->stop();
//
//  auto tupleSet = TupleSet2::create(tuples);
//
//  SPDLOG_INFO("Metrics:\n{}", mgr->showMetrics());
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  SPDLOG_INFO("Finish");
}

TEST_CASE ("Join_Three" * doctest::skip(true || SKIP_SUITE)) {
//  normal::pushdown::AWSClient client;
//  client.init();
//
//  // operators
//  auto s3Bucket = "s3filter";
//  std::vector<std::string> s3Objects = {"ssb-sf0.01/part.tbl", "ssb-sf0.01/lineorder.tbl", "ssb-sf0.01/supplier.tbl"};
//  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
//
//  // lineorder scan
//  auto numBytes = partitionMap.find("ssb-sf0.01/lineorder.tbl")->second;
//  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  std::vector<std::string> columns = {"lo_orderkey", "lo_partkey", "lo_suppkey"};
//  auto lineorderScan = normal::pushdown::S3SelectScan::make(
//          "SimpleScan",
//          "s3filter",
//          "ssb-sf0.01/lineorder.tbl",
//          fmt::format("select lo_orderkey, lo_partkey, lo_suppkey from s3Object"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // part scan
//  numBytes = partitionMap.find("ssb-sf0.01/part.tbl")->second;
//  scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  columns = {"p_partkey", "p_name"};
//  auto partScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/part.tbl",
//          "s3filter",
//          "ssb-sf0.01/part.tbl",
//          fmt::format("select p_partkey, p_name from s3Object where (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // supplier scan
//  numBytes = partitionMap.find("ssb-sf0.01/supplier.tbl")->second;
//  scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  columns = {"s_suppkey", "s_nation"};
//  auto supplierScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/supplier.tbl",
//          "s3filter",
//          "ssb-sf0.01/supplier.tbl",
//          fmt::format("select s_suppkey, s_nation from s3Object"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // shuffle
//  auto partShuffle = normal::pushdown::shuffle::Shuffle::make("partShuffle", "p_partkey");
//  auto lineorderShuffle = normal::pushdown::shuffle::Shuffle::make("lineorderShuffle", "lo_partkey");
//  auto join1Shuffle = normal::pushdown::shuffle::Shuffle::make("join1Shuffle", "lo_suppkey");
//  auto supplierShuffle = normal::pushdown::shuffle::Shuffle::make("supplierShuffle", "s_suppkey");
//
//  // join
//  auto joinBuild1 = std::make_shared<normal::pushdown::join::HashJoinBuild>("join-build-1", "p_partkey");
//  auto joinProbe1 = std::make_shared<normal::pushdown::join::HashJoinProbe>("join-probe-1",
//          normal::pushdown::join::JoinPredicate::create("p_partkey","lo_partkey"));
//  auto joinBuild2 = std::make_shared<normal::pushdown::join::HashJoinBuild>("join-build-2", "lo_suppkey");
//  auto joinProbe2 = std::make_shared<normal::pushdown::join::HashJoinProbe>("join-probe-2",
//          normal::pushdown::join::JoinPredicate::create("lo_suppkey","s_suppkey"));
//
//  // collate
//  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);
//
//  // wire up
//  auto mgr = std::make_shared<OperatorManager>();
//
//  // scan
//  partScan->produce(partShuffle);
//  partShuffle->consume(partScan);
//
//  lineorderScan->produce(lineorderShuffle);
//  lineorderShuffle->produce(lineorderScan);
//
//  supplierScan->produce(supplierShuffle);
//  supplierShuffle->consume(supplierScan);
//
//  // first join
//  partShuffle->produce(joinBuild1);
//  joinBuild1->consume(partShuffle);
//
//  lineorderShuffle->produce(joinProbe1);
//  joinProbe1->consume(lineorderShuffle);
//
//  joinBuild1->produce(joinProbe1);
//  joinProbe1->consume(joinBuild1);
//
//  // second join
//  joinProbe1->produce(join1Shuffle);
//  join1Shuffle->consume(joinProbe1);
//
//  join1Shuffle->produce(joinBuild2);
//  joinBuild2->consume(join1Shuffle);
//
//  joinBuild2->produce(joinProbe2);
//  joinProbe2->consume(joinBuild2);
//
//  supplierShuffle->produce(joinProbe2);
//  joinProbe2->consume(supplierShuffle);
//
//  // collate
//  joinProbe2->produce(collate);
//  collate->consume(joinProbe2);
//
//  mgr->put(partScan);
//  mgr->put(lineorderScan);
//  mgr->put(supplierScan);
//  mgr->put(partShuffle);
//  mgr->put(lineorderShuffle);
//  mgr->put(join1Shuffle);
//  mgr->put(supplierShuffle);
//  mgr->put(joinBuild1);
//  mgr->put(joinProbe1);
//  mgr->put(joinBuild2);
//  mgr->put(joinProbe2);
//  mgr->put(collate);
//
//  // execute
//  mgr->boot();
//  mgr->start();
//  mgr->join();
//  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(mgr->getOperator("collate"))->tuples();
//  mgr->stop();
//
//  SPDLOG_INFO("Finish");
}

TEST_CASE ("Join_Two_Aggregate" * doctest::skip(true || SKIP_SUITE)) {
//  normal::pushdown::AWSClient client;
//  client.init();
//
//  // operators
//  auto s3Bucket = "s3filter";
//  std::vector<std::string> s3Objects = {"ssb-sf0.01/date.tbl", "ssb-sf0.01/lineorder.tbl"};
//  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
//
//  // lineorder scan
//  auto numBytes = partitionMap.find("ssb-sf0.01/lineorder.tbl")->second;
//  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  std::vector<std::string> columns = {"lo_extendedprice", "lo_discount", "lo_orderdate"};
//  auto lineorderScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/lineorder.tbl",
//          "s3filter",
//          "ssb-sf0.01/lineorder.tbl",
//          fmt::format("select lo_extendedprice, lo_discount, lo_orderdate from s3Object "
//                      "where cast(lo_discount as int) between 1 and 3 and cast(lo_quantity as int) < 25"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // date scan
//  numBytes = partitionMap.find("ssb-sf0.01/date.tbl")->second;
//  scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  columns = {"d_datekey"};
//  auto dateScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/date.tbl",
//          "s3filter",
//          "ssb-sf0.01/date.tbl",
//          fmt::format("select d_datekey from s3Object where cast(d_year as int) = 1992"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // shuffle
//  auto dateShuffle = normal::pushdown::shuffle::Shuffle::make("dateShuffle", "d_datekey");
//  auto lineorderShuffle = normal::pushdown::shuffle::Shuffle::make("lineorderShuffle", "lo_orderdate");
//
//  // join
//  auto joinBuild = std::make_shared<normal::pushdown::join::HashJoinBuild>("join-build", "d_datekey");
//  auto joinProbe = std::make_shared<normal::pushdown::join::HashJoinProbe>("join-probe",
//                                                                           normal::pushdown::join::JoinPredicate::create("d_datekey","lo_orderdate"));
//
//  // aggregate
//  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
//  aggregateFunctions->
//          emplace_back(std::make_shared<aggregate::Sum>("revenue",
//                  times(cast(col("lo_extendedprice"), normal::core::type::float64Type()),
//                          cast(col("lo_discount"), normal::core::type::float64Type()))
//  ));
//  auto aggregate = std::make_shared<normal::pushdown::Aggregate>("aggregate", aggregateFunctions);
//
//  // collate
//  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);
//
//  // wire up
//  auto mgr = std::make_shared<OperatorManager>();
//
////  dateScan->produce(dateShuffle);
////  dateShuffle->consume(dateScan);
////
////  lineorderScan->produce(lineorderShuffle);
////  lineorderShuffle->consume(lineorderScan);
////
////  dateShuffle->produce(joinBuild);
////  joinBuild->consume(dateShuffle);
////
////  lineorderShuffle->produce(joinProbe);
////  joinProbe->consume(lineorderShuffle);
//
//  dateScan->produce(joinBuild);
//  joinBuild->consume(dateScan);
//
//  lineorderScan->produce(joinProbe);
//  joinProbe->consume(lineorderScan);
//
//  joinBuild->produce(joinProbe);
//  joinProbe->consume(joinBuild);
//
//  joinProbe->produce(aggregate);
//  aggregate->consume(joinProbe);
//
//  aggregate->produce(collate);
//  collate->consume(aggregate);
//
//  mgr->put(dateScan);
//  mgr->put(lineorderScan);
////  mgr->put(dateShuffle);
////  mgr->put(lineorderShuffle);
//  mgr->put(joinBuild);
//  mgr->put(joinProbe);
//  mgr->put(aggregate);
//  mgr->put(collate);
//
//  // execute
//  mgr->boot();
//  mgr->start();
//  mgr->join();
//  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(mgr->getOperator("collate"))->tuples();
//  mgr->stop();
//
//  auto tupleSet = TupleSet2::create(tuples);
//
//  SPDLOG_INFO("Metrics:\n{}", mgr->showMetrics());
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  SPDLOG_INFO("Finish");
}

TEST_CASE ("Join_Three_Aggregate" * doctest::skip(true || SKIP_SUITE)) {
//  normal::pushdown::AWSClient client;
//  client.init();
//
//  // operators
//  auto s3Bucket = "s3filter";
//  std::vector<std::string> s3Objects = {"ssb-sf0.01/date.tbl", "ssb-sf0.01/lineorder.tbl", "ssb-sf0.01/part.tbl"};
//  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
//
//  // lineorder scan
//  auto numBytes = partitionMap.find("ssb-sf0.01/lineorder.tbl")->second;
//  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  std::vector<std::string> columns = {"lo_orderkey", "lo_orderdate", "lo_partkey", "lo_extendedprice", "lo_discount"};
//  auto lineorderScan = normal::pushdown::S3SelectScan::make(
//          "SimpleScan",
//          "s3filter",
//          "ssb-sf0.01/lineorder.tbl",
//          fmt::format("select lo_orderkey, lo_orderdate, lo_partkey, lo_extendedprice, lo_discount from s3Object"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // date scan
//  numBytes = partitionMap.find("ssb-sf0.01/date.tbl")->second;
//  scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  columns = {"d_datekey", "d_year"};
//  auto dateScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/date.tbl",
//          "s3filter",
//          "ssb-sf0.01/date.tbl",
//          fmt::format("select d_datekey, d_year from s3Object"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // part scan
//  numBytes = partitionMap.find("ssb-sf0.01/part.tbl")->second;
//  scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  columns = {"p_partkey", "p_name"};
//  auto partScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/part.tbl",
//          "s3filter",
//          "ssb-sf0.01/part.tbl",
//          fmt::format("select p_partkey, p_name from s3Object"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // shuffle
//  auto dateShuffle = normal::pushdown::shuffle::Shuffle::make("dateShuffle", "d_datekey");
//  auto lineorderShuffle = normal::pushdown::shuffle::Shuffle::make("lineorderShuffle", "lo_orderdate");
//  auto join1Shuffle = normal::pushdown::shuffle::Shuffle::make("join1Shuffle", "lo_partkey");
//  auto partShuffle = normal::pushdown::shuffle::Shuffle::make("partShuffle", "p_partkey");
//
//  // join
//  auto joinBuild1 = std::make_shared<normal::pushdown::join::HashJoinBuild>("join-build-1", "d_datekey");
//  auto joinProbe1 = std::make_shared<normal::pushdown::join::HashJoinProbe>("join-probe-1",
//                                                                            normal::pushdown::join::JoinPredicate::create("d_datekey","lo_orderdate"));
//  auto joinBuild2 = std::make_shared<normal::pushdown::join::HashJoinBuild>("join-build-2", "lo_partkey");
//  auto joinProbe2 = std::make_shared<normal::pushdown::join::HashJoinProbe>("join-probe-2",
//                                                                            normal::pushdown::join::JoinPredicate::create("lo_partkey","p_partkey"));
//
//  // aggregate
//  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
//  aggregateFunctions->
//          emplace_back(std::make_shared<aggregate::Sum>("revenue",
//                                                        times(cast(col("lo_extendedprice"), normal::core::type::float64Type()),
//                                                              cast(col("lo_discount"), normal::core::type::float64Type()))
//  ));
//  auto aggregate = std::make_shared<normal::pushdown::Aggregate>("aggregate", aggregateFunctions);
//
//  // collate
//  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);
//
//  // wire up
//  auto mgr = std::make_shared<OperatorManager>();
//
//  // scan
//  dateScan->produce(dateShuffle);
//  dateShuffle->consume(dateScan);
//
//  lineorderScan->produce(lineorderShuffle);
//  lineorderShuffle->produce(lineorderScan);
//
//  partScan->produce(partShuffle);
//  partShuffle->consume(partScan);
//
//  // first join
//  dateShuffle->produce(joinBuild1);
//  joinBuild1->consume(dateShuffle);
//
//  lineorderShuffle->produce(joinProbe1);
//  joinProbe1->consume(lineorderShuffle);
//
//  joinBuild1->produce(joinProbe1);
//  joinProbe1->consume(joinBuild1);
//
//  // second join
//  joinProbe1->produce(join1Shuffle);
//  join1Shuffle->consume(joinProbe1);
//
//  join1Shuffle->produce(joinBuild2);
//  joinBuild2->consume(join1Shuffle);
//
//  joinBuild2->produce(joinProbe2);
//  joinProbe2->consume(joinBuild2);
//
//  partShuffle->produce(joinProbe2);
//  joinProbe2->consume(partShuffle);
//
//  // aggregate
//  joinProbe2->produce(aggregate);
//  aggregate->consume(joinProbe2);
//
//  // collate
//  aggregate->produce(collate);
//  collate->consume(aggregate);
//
//  mgr->put(dateScan);
//  mgr->put(lineorderScan);
//  mgr->put(partScan);
//  mgr->put(dateShuffle);
//  mgr->put(lineorderShuffle);
//  mgr->put(join1Shuffle);
//  mgr->put(partShuffle);
//  mgr->put(joinBuild1);
//  mgr->put(joinProbe1);
//  mgr->put(joinBuild2);
//  mgr->put(joinProbe2);
//  mgr->put(aggregate);
//  mgr->put(collate);
//
//  // execute
//  mgr->boot();
//  mgr->start();
//  mgr->join();
//  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(mgr->getOperator("collate"))->tuples();
//  mgr->stop();
//
//  auto tupleSet = TupleSet2::create(tuples);
//
//  SPDLOG_INFO("Metrics:\n{}", mgr->showMetrics());
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  SPDLOG_INFO("Finish");
}

TEST_CASE ("Join_Two_Group" * doctest::skip(true || SKIP_SUITE)) {
//  normal::pushdown::AWSClient client;
//  client.init();
//
//  // operators
//  auto s3Bucket = "s3filter";
//  std::vector<std::string> s3Objects = {"ssb-sf0.01/date.tbl", "ssb-sf0.01/lineorder.tbl"};
//  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
//
//  // lineorder scan
//  auto numBytes = partitionMap.find("ssb-sf0.01/lineorder.tbl")->second;
//  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  std::vector<std::string> columns = {"lo_extendedprice", "lo_discount", "lo_orderdate", "lo_shipmode"};
//  auto lineorderScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/lineorder.tbl",
//          "s3filter",
//          "ssb-sf0.01/lineorder.tbl",
//          fmt::format("select lo_extendedprice, lo_discount, lo_orderdate, lo_shipmode from s3Object "
//                      "where cast(lo_discount as int) between 1 and 3 and cast(lo_quantity as int) < 25"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // date scan
//  numBytes = partitionMap.find("ssb-sf0.01/date.tbl")->second;
//  scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  columns = {"d_datekey", "d_weeknuminyear"};
//  auto dateScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/date.tbl",
//          "s3filter",
//          "ssb-sf0.01/date.tbl",
//          fmt::format("select d_datekey, d_weeknuminyear from s3Object where cast(d_year as int) = 1992"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // shuffle
//  auto dateShuffle = normal::pushdown::shuffle::Shuffle::make("dateShuffle", "d_datekey");
//  auto lineorderShuffle = normal::pushdown::shuffle::Shuffle::make("lineorderShuffle", "lo_orderdate");
//
//  // join
//  auto joinBuild = std::make_shared<normal::pushdown::join::HashJoinBuild>("join-build", "d_datekey");
//  auto joinProbe = std::make_shared<normal::pushdown::join::HashJoinProbe>("join-probe",
//                                                                           normal::pushdown::join::JoinPredicate::create("d_datekey","lo_orderdate"));
//
//  // aggregate
//  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
//  aggregateFunctions->
//          emplace_back(std::make_shared<aggregate::Sum>("revenue",
//                                                        times(cast(col("lo_extendedprice"), normal::core::type::float64Type()),
//                                                              cast(col("lo_discount"), normal::core::type::float64Type()))
//  ));
//  std::vector<std::string> groupColumns = {"d_weeknuminyear", "lo_shipmode"};
//  auto group = std::make_shared<normal::pushdown::group::Group>("group", groupColumns, aggregateFunctions);
//
//  // collate
//  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);
//
//  // wire up
//  auto mgr = std::make_shared<OperatorManager>();
//
//  dateScan->produce(dateShuffle);
//  dateShuffle->consume(dateScan);
//
//  lineorderScan->produce(lineorderShuffle);
//  lineorderShuffle->consume(lineorderScan);
//
//  dateShuffle->produce(joinBuild);
//  joinBuild->consume(dateShuffle);
//
//  lineorderShuffle->produce(joinProbe);
//  joinProbe->consume(lineorderShuffle);
//
//  joinBuild->produce(joinProbe);
//  joinProbe->consume(joinBuild);
//
//  joinProbe->produce(group);
//  group->consume(joinProbe);
//
//  group->produce(collate);
//  collate->consume(group);
//
//  mgr->put(dateScan);
//  mgr->put(lineorderScan);
//  mgr->put(dateShuffle);
//  mgr->put(lineorderShuffle);
//  mgr->put(joinBuild);
//  mgr->put(joinProbe);
//  mgr->put(group);
//  mgr->put(collate);
//
//  // execute
//  mgr->boot();
//  mgr->start();
//  mgr->join();
//  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(mgr->getOperator("collate"))->tuples();
//  mgr->stop();
//
//  auto tupleSet = TupleSet2::create(tuples);
//
//  SPDLOG_INFO("Metrics:\n{}", mgr->showMetrics());
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  SPDLOG_INFO("Finish");
}

TEST_CASE ("Join_Two_Project" * doctest::skip(true || SKIP_SUITE)) {
//  normal::pushdown::AWSClient client;
//  client.init();
//
//  // operators
//  auto s3Bucket = "s3filter";
//  std::vector<std::string> s3Objects = {"ssb-sf0.01/date.tbl", "ssb-sf0.01/lineorder.tbl"};
//  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
//
//  // lineorder scan
//  auto numBytes = partitionMap.find("ssb-sf0.01/lineorder.tbl")->second;
//  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  std::vector<std::string> columns = {"lo_orderdate", "lo_quantity"};
//  auto lineorderScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/lineorder.tbl",
//          "s3filter",
//          "ssb-sf0.01/lineorder.tbl",
//          fmt::format("select lo_orderdate, lo_quantity from s3Object "
//                      "where cast(lo_discount as int) between 1 and 3 and cast(lo_quantity as int) < 25"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // date scan
//  numBytes = partitionMap.find("ssb-sf0.01/date.tbl")->second;
//  scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  columns = {"d_datekey", "d_weeknuminyear"};
//  auto dateScan = normal::pushdown::S3SelectScan::make(
//          "s3filter/ssb-sf0.01/date.tbl",
//          "s3filter",
//          "ssb-sf0.01/date.tbl",
//          fmt::format("select d_datekey, d_weeknuminyear from s3Object where cast(d_year as int) = 1992"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
//          true);
//
//  // shuffle
//  auto dateShuffle = normal::pushdown::shuffle::Shuffle::make("dateShuffle", "d_datekey");
//  auto lineorderShuffle = normal::pushdown::shuffle::Shuffle::make("lineorderShuffle", "lo_orderdate");
//
//  // join
//  auto joinBuild = std::make_shared<normal::pushdown::join::HashJoinBuild>("join-build", "d_datekey");
//  auto joinProbe = std::make_shared<normal::pushdown::join::HashJoinProbe>("join-probe",
//                                                                           normal::pushdown::join::JoinPredicate::create("d_datekey","lo_orderdate"));
//
//  // project
//  auto projectExpressions = std::make_shared<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>>();
//  projectExpressions->emplace_back(col("lo_orderdate"));
//  projectExpressions->emplace_back(col("lo_quantity"));
//  projectExpressions->emplace_back(col("d_weeknuminyear"));
//  auto project = std::make_shared<normal::pushdown::Project>("project", *projectExpressions);
//
//  // collate
//  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);
//
//  // wire up
//  auto mgr = std::make_shared<OperatorManager>();
//
//  dateScan->produce(dateShuffle);
//  dateShuffle->consume(dateScan);
//
//  lineorderScan->produce(lineorderShuffle);
//  lineorderShuffle->consume(lineorderScan);
//
//  dateShuffle->produce(joinBuild);
//  joinBuild->consume(dateShuffle);
//
//  lineorderShuffle->produce(joinProbe);
//  joinProbe->consume(lineorderShuffle);
//
//  joinBuild->produce(joinProbe);
//  joinProbe->consume(joinBuild);
//
//  joinProbe->produce(project);
//  project->consume(joinProbe);
//
//  project->produce(collate);
//  collate->consume(project);
//
//  mgr->put(dateScan);
//  mgr->put(lineorderScan);
//  mgr->put(dateShuffle);
//  mgr->put(lineorderShuffle);
//  mgr->put(joinBuild);
//  mgr->put(joinProbe);
//  mgr->put(project);
//  mgr->put(collate);
//
//  // execute
//  mgr->boot();
//  mgr->start();
//  mgr->join();
//  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(mgr->getOperator("collate"))->tuples();
//  mgr->stop();
//
//  auto tupleSet = TupleSet2::create(tuples);
//
//  SPDLOG_INFO("Metrics:\n{}", mgr->showMetrics());
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  SPDLOG_INFO("Finish");
}

}