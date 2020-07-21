//
// Created by Yifei Yang on 7/7/20.
//

#include <doctest/doctest.h>
#include <normal/sql/Interpreter.h>
#include <normal/ssb/TestUtil.h>
#include <normal/pushdown/Collate.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <normal/pushdown/shuffle/Shuffle.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
#include <normal/connector/s3/S3SelectCatalogueEntry.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/pushdown/Util.h>
#include "ExperimentUtil.h"

#define SKIP_SUITE false

using namespace normal::ssb;

void configureS3Connector(normal::sql::Interpreter &i, std::string bucket_name, std::string dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  // look up tables
  std::vector<std::string> object_keys = ExperimentUtil::list_objects(*conn->getAwsClient(), bucket_name, dir_prefix);
  std::vector<std::string> table_names;
  for (const auto &object_key: object_keys) {
    std::string table_name = object_key.substr(dir_prefix.size(), object_key.size());
    if (table_name.size() > 0) {
      table_names.push_back(table_name);
    }
  }

  // configure s3Connector
  for (const auto &table_name: table_names) {
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    partitioningScheme->add(std::make_shared<S3SelectPartition>("s3filter", dir_prefix + table_name));
    cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>(table_name.substr(0, table_name.size() - 4), partitioningScheme, cat));
  }
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

auto executeSql(normal::sql::Interpreter i, const std::string &sql) {
  i.parse(sql);

  TestUtil::writeExecutionPlan(*i.getLogicalPlan());
  TestUtil::writeExecutionPlan(*i.getOperatorManager());

  auto tuples = execute(i);

  i.getOperatorManager()->stop();

  SPDLOG_INFO("Metrics:\n{}", i.getOperatorManager()->showMetrics());

  return tuples;
}

TEST_CASE ("FullPushdown-SequentialRun" * doctest::skip(true || SKIP_SUITE)) {
  // hardcoded parameters
  std::vector<std::string> sql_file_names = {
          "query4.1.sql"
  };
  auto currentPath = filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql");
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf1/";

  // configure interpreter
  normal::sql::Interpreter i;
  configureS3Connector(i, bucket_name, dir_prefix);

  // execute
  for (const auto &sql_file_name: sql_file_names) {
    // read sql file
    auto sql_file_path = sql_file_dir_path.append(sql_file_name);
    auto sql = ExperimentUtil::read_file(sql_file_path.string());
    SPDLOG_INFO("Sql: \n{}", sql);

    // execute sql
    executeSql(i, sql);
  }

  SPDLOG_INFO("Finish");
}

TEST_CASE ("SimpleScan" * doctest::skip(true || SKIP_SUITE)) {
  normal::pushdown::AWSClient client;
  client.init();

  // operators
  auto s3Bucket = "s3filter";
  auto s3Object = "ssb-sf0.01/part.tbl";
  std::vector<std::string> s3Objects = {s3Object};
  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
  auto numBytes = partitionMap.find(s3Object)->second;
  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
  std::vector<std::string> columns = {"p_partkey"};
  auto lineorderScan = normal::pushdown::S3SelectScan::make(
          "SimpleScan",
          "s3filter",
          s3Object,
          fmt::format("select p_partkey from s3Object where (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')"),
          columns,
          scanRanges[0].first,
          scanRanges[0].second,
          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
          client.defaultS3Client());

  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);

  // wire up
  auto mgr = std::make_shared<OperatorManager>();
  lineorderScan->produce(collate);
  collate->consume(lineorderScan);
  mgr->put(lineorderScan);
  mgr->put(collate);

  // execute
  mgr->boot();
  mgr->start();
  mgr->join();
  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(mgr->getOperator("collate"))->tuples();
  mgr->stop();

  SPDLOG_INFO("Finish");
}

TEST_CASE ("ScanJoin" * doctest::skip(false || SKIP_SUITE)) {
  normal::pushdown::AWSClient client;
  client.init();

  // operators
  auto s3Bucket = "s3filter";
  std::vector<std::string> s3Objects = {"ssb-sf0.01/part.tbl", "ssb-sf0.01/lineorder.tbl"};
  auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());

  // lineorder scan
  auto numBytes = partitionMap.find("ssb-sf0.01/lineorder.tbl")->second;
  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
  std::vector<std::string> columns = {"lo_orderkey", "lo_partkey"};
  auto lineorderScan = normal::pushdown::S3SelectScan::make(
          "SimpleScan",
          "s3filter",
          "ssb-sf0.01/lineorder.tbl",
          fmt::format("select lo_orderkey, lo_partkey from s3Object"),
          columns,
          scanRanges[0].first,
          scanRanges[0].second,
          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
          client.defaultS3Client());

  // part scan
  numBytes = partitionMap.find("ssb-sf0.01/part.tbl")->second;
  scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
  columns = {"p_partkey", "p_name"};
  auto partScan = normal::pushdown::S3SelectScan::make(
          "s3filter/ssb-sf0.01/part.tbl",
          "s3filter",
          "ssb-sf0.01/part.tbl",
          fmt::format("select p_partkey, p_name from s3Object where (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')"),
          columns,
          scanRanges[0].first,
          scanRanges[0].second,
          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
          client.defaultS3Client());

  // shuffle
  auto partShuffle = normal::pushdown::shuffle::Shuffle::make("partShuffle", "p_partkey");
  auto lineorderShuffle = normal::pushdown::shuffle::Shuffle::make("lineorderShuffle", "lo_partkey");

  // join
  auto joinBuild = std::make_shared<normal::pushdown::join::HashJoinBuild>("join-build", "p_partkey");
  auto joinProbe = std::make_shared<normal::pushdown::join::HashJoinProbe>("join-probe-{}",
          normal::pushdown::join::JoinPredicate::create("p_partkey","lo_partkey"));

  // collate
  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);

  // wire up
  auto mgr = std::make_shared<OperatorManager>();

  partScan->produce(partShuffle);
  partShuffle->consume(partScan);

  lineorderScan->produce(lineorderShuffle);
  lineorderShuffle->produce(lineorderScan);

  partShuffle->produce(joinBuild);
  joinBuild->consume(partShuffle);

  lineorderShuffle->produce(joinProbe);
  joinProbe->consume(lineorderShuffle);

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  joinProbe->produce(collate);
  collate->consume(joinProbe);

  mgr->put(partScan);
  mgr->put(lineorderScan);
  mgr->put(partShuffle);
  mgr->put(lineorderShuffle);
  mgr->put(joinBuild);
  mgr->put(joinProbe);
  mgr->put(collate);

  // execute
  mgr->boot();
  mgr->start();
  mgr->join();
  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(mgr->getOperator("collate"))->tuples();
  mgr->stop();

  SPDLOG_INFO("Finish");
}