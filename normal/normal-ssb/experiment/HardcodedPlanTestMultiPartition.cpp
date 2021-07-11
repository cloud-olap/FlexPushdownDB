//
// Created by Yifei Yang on 7/20/20.
//

#include <doctest/doctest.h>
#include <normal/ssb/TestUtil.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <normal/pushdown/shuffle/Shuffle.h>
#include <normal/pushdown/aggregate/AggregationFunction.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/pushdown/Util.h>
#include <normal/pushdown/aggregate/Aggregate.h>
#include <normal/pushdown/project/Project.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/pushdown/group/Group.h>
#include "ExperimentUtil.h"

#define SKIP_SUITE true

using namespace normal::ssb;
using namespace normal::pushdown;
using namespace normal::expression::gandiva;

TEST_SUITE ("Hardcoded-plan-test-multi-partition" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("SimpleScan" * doctest::skip(false || SKIP_SUITE)) {
//  normal::pushdown::AWSClient client;
//  client.init();
//
//  // operators
//  auto s3Bucket = "s3filter";
//  auto s3Object = "ssb-sf1/lineorder.tbl";
//  std::vector<std::string> s3Objects = {s3Object};
//  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
//  auto numBytes = partitionMap.find(s3Object)->second;
//  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//  std::vector<std::string> columns = {"lo_orderkey", "lo_orderdate", "lo_extendedprice"};
//  auto lineorderScan = normal::pushdown::S3SelectScan::make(
//          "SimpleScan",
//          "s3filter",
//          s3Object,
//          fmt::format("select lo_orderkey, lo_orderdate, lo_extendedprice from s3Object"),
//          columns,
//          scanRanges[0].first,
//          scanRanges[0].second,
//          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//          client.defaultS3Client(),
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

TEST_CASE ("SimpleScan-partitioned" * doctest::skip(false || SKIP_SUITE)) {
//  normal::pushdown::AWSClient client;
//  client.init();
//  int numPartitions = 32;
//
//  // operators
//  auto s3Bucket = "s3filter";
//  std::vector<std::string> s3Objects;
//  for (int i = 0; i < numPartitions; i++) {
//    s3Objects.emplace_back(fmt::format("ssb-sf1/lineorder_sharded/lineorder.tbl.{}", i));
//  }
//  std::vector<std::string> columns = {"lo_orderkey", "lo_orderdate", "lo_extendedprice"};
//  auto sql = fmt::format("select lo_orderkey, lo_orderdate, lo_extendedprice from s3Object");
//  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
//  std::vector<std::shared_ptr<normal::pushdown::S3SelectScan>> lineorderScans;
//
//  for (int i = 0; i < numPartitions; i++) {
//    auto s3Object = s3Objects[i];
//    auto numBytes = partitionMap.find(s3Object)->second;
//    auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
//    auto lineorderScan = normal::pushdown::S3SelectScan::make(
//            fmt::format("lineorderScan-{}", i),
//            "s3filter",
//            s3Object,
//            sql,
//            columns,
//            scanRanges[0].first,
//            scanRanges[0].second,
//            normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
//            client.defaultS3Client(),
//            true);
//    lineorderScans.emplace_back(lineorderScan);
//  }
//
//  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);
//  auto mgr = std::make_shared<OperatorManager>();
//
//  // wire up
//  for (int i = 0; i < numPartitions; i++) {
//    lineorderScans[i]->produce(collate);
//    collate->consume(lineorderScans[i]);
//  }
//
//  // add to OperatorManger
//  for (int i = 0; i < numPartitions; i++) {
//    mgr->put(lineorderScans[i]);
//  }
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

}