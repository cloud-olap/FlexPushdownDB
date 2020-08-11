//
// Created by Yifei Yang on 7/7/20.
//

#include <doctest/doctest.h>
#include <normal/sql/Interpreter.h>
#include <normal/ssb/TestUtil.h>
#include <normal/pushdown/Collate.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
#include <normal/connector/s3/S3SelectCatalogueEntry.h>
#include <normal/pushdown/Util.h>
#include <normal/plan/mode/Modes.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/cache/LRUCachingPolicy.h>
#include <normal/cache/FBRCachingPolicy.h>
#include "ExperimentUtil.h"
#include <normal/ssb/SqlGenerator.h>

#define SKIP_SUITE false

using namespace normal::ssb;

void configureS3ConnectorSinglePartition(normal::sql::Interpreter &i, std::string bucket_name, std::string dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  // look up tables
  std::vector<std::string> tableNames = {"lineorder", "date", "customer", "supplier", "part"};
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  for (const auto &tableName: tableNames) {
    auto s3Object = dir_prefix + tableName + ".tbl";
    s3Objects->emplace_back(s3Object);
  }
  auto client = conn->getAwsClient();
  client->init();
  auto objectNumBytes_Map = normal::connector::s3::S3Util::listObjects(bucket_name, *s3Objects, client->defaultS3Client());

  // configure s3Connector
  for (int tbl_id = 0; tbl_id < tableNames.size(); tbl_id++) {
    auto &tableName = tableNames[tbl_id];
    auto &s3Object = s3Objects->at(tbl_id);
    auto numBytes = objectNumBytes_Map.find(s3Object)->second;
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    partitioningScheme->add(std::make_shared<S3SelectPartition>("s3filter", s3Object, numBytes));
    cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
  }
  i.put(cat);
}

void configureS3ConnectorMultiPartition(normal::sql::Interpreter &i, std::string bucket_name, std::string dir_prefix, int partitionNum) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  // s3Objects map
  auto s3ObjectsMap = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>();
  auto lineorderS3Objects = std::make_shared<std::vector<std::string>>();
  for (int i = 0; i < partitionNum; i++) {
    lineorderS3Objects->emplace_back(fmt::format("{}lineorder_sharded/lineorder.tbl.{}", dir_prefix, i));
  }
  auto dateS3Objects = std::make_shared<std::vector<std::string>>(std::vector<std::string>({dir_prefix + "date.tbl"}));
  auto customerS3Objects = std::make_shared<std::vector<std::string>>(std::vector<std::string>({dir_prefix + "customer.tbl"}));
  auto supplierS3Objects = std::make_shared<std::vector<std::string>>(std::vector<std::string>({dir_prefix + "supplier.tbl"}));
  auto partS3Objects = std::make_shared<std::vector<std::string>>(std::vector<std::string>({dir_prefix + "part.tbl"}));
  s3ObjectsMap->insert({"lineorder", lineorderS3Objects});
  s3ObjectsMap->insert({"date", dateS3Objects});
  s3ObjectsMap->insert({"customer", customerS3Objects});
  s3ObjectsMap->insert({"supplier", supplierS3Objects});
  s3ObjectsMap->insert({"part", partS3Objects});

  // look up tables
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto objects = s3ObjectPair.second;
    s3Objects->insert(s3Objects->end(), objects->begin(), objects->end());
  }
  auto client = conn->getAwsClient();
  client->init();
  auto objectNumBytes_Map = normal::connector::s3::S3Util::listObjects(bucket_name, *s3Objects, client->defaultS3Client());

  // configure s3Connector
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto tableName = s3ObjectPair.first;
    auto objects = s3ObjectPair.second;
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    for (auto const &s3Object: *objects) {
      auto numBytes = objectNumBytes_Map.find(s3Object)->second;
      partitioningScheme->add(std::make_shared<S3SelectPartition>("s3filter", s3Object, numBytes));
    }
    cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
  }
  i.put(cat);
}

auto execute(normal::sql::Interpreter &i) {
  i.getOperatorGraph()->boot();
  i.getOperatorGraph()->start();
  i.getOperatorGraph()->join();

  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(i.getOperatorGraph()->getOperator("collate"))->tuples();

  return tuples;
}

auto executeSql(normal::sql::Interpreter &i, const std::string &sql, bool saveMetrics) {
  i.clearOperatorGraph();
  i.parse(sql);

  TestUtil::writeExecutionPlan(*i.getLogicalPlan());
  TestUtil::writeExecutionPlan2(*i.getOperatorGraph());

  auto tuples = execute(i);

  auto tupleSet = TupleSet2::create(tuples);
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  SPDLOG_INFO("Metrics:\n{}", i.getOperatorGraph()->showMetrics());
//  SPDLOG_INFO("Finish");
  SPDLOG_INFO("Finished, time: {} secs", (double) (i.getOperatorGraph()->getElapsedTime().value()) / 1000000000.0);
  if (saveMetrics) {
    i.saveMetrics();
  }

  return tupleSet;
}

TEST_SUITE ("MainTests" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("SequentialRun" * doctest::skip(true || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);

  // choose whether to use partitioned lineorder
  bool partitioned = true;

  // choose mode
  auto mode1 = normal::plan::operator_::mode::Modes::fullPushdownMode();
  auto mode2 = normal::plan::operator_::mode::Modes::pullupCachingMode();
  auto mode3 = normal::plan::operator_::mode::Modes::hybridCachingMode();

  // hardcoded parameters
  std::vector<std::string> sql_file_names = {
//          "query1.1.sql", "query1.2.sql", "query1.3.sql",
//          "query2.1.sql", "query2.2.sql", "query2.3.sql",
//          "query3.1.sql", "query3.2.sql", "query3.3.sql", "query3.4.sql",
//          "query4.1.sql", "query4.2.sql", "query4.3.sql"
          "query1.1.1.sql"
  };
  auto currentPath = filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql");
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf1/";

  // choose caching policy
  auto lru = LRUCachingPolicy::make(1024*1024*600);
  auto fbr = FBRCachingPolicy::make(1024*1024*600);

  // configure interpreter
  normal::sql::Interpreter i(mode1, lru);
  if (partitioned) {
    configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix, 32);
  } else {
    configureS3ConnectorSinglePartition(i, bucket_name, dir_prefix);
  }

  // execute
  i.boot();

  for (const auto &sql_file_name: sql_file_names) {
    // read sql file
    auto sql_file_path = sql_file_dir_path.append(sql_file_name);
    SPDLOG_DEBUG(sql_file_dir_path.string());
    auto sql = ExperimentUtil::read_file(sql_file_path.string());
    SPDLOG_INFO("{}: \n{}", sql_file_name, sql);

    // execute sql
    executeSql(i, sql, true);
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }

  i.stop();

  SPDLOG_INFO("Sequence all finished");
  SPDLOG_INFO("Overall Metrics:\n{}", i.showMetrics());
}

TEST_CASE ("GenerateSqlBatchRun" * doctest::skip(true || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);

  // prepare queries
  SqlGenerator sqlGenerator;
  auto sqls = sqlGenerator.generateSqlBatch(100);

  // prepare interpreter
  bool partitioned = false;
  auto mode = normal::plan::operator_::mode::Modes::fullPushdownMode();
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf0.01/";
  normal::sql::Interpreter i;
  if (partitioned) {
    configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix, 10);
  } else {
    configureS3ConnectorSinglePartition(i, bucket_name, dir_prefix);
  }

  // execute
  i.boot();
  int index = 1;
  for (const auto &sql: sqls) {
    SPDLOG_INFO("sql {}: \n{}", index++, sql);
    executeSql(i, sql, true);
  }
  i.stop();

  SPDLOG_INFO("Batch finished");
}

TEST_CASE ("ColdCacheExperiment" * doctest::skip(true || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);

  // parameters
  const int batchSize = 5;
  const size_t cacheSize = 1024*1024*100;
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf1/";
  const int partitionNum = 32;
  auto mode1 = normal::plan::operator_::mode::Modes::fullPushdownMode();
  auto mode2 = normal::plan::operator_::mode::Modes::pullupCachingMode();
  auto mode3 = normal::plan::operator_::mode::Modes::hybridCachingMode();
  auto lru = LRUCachingPolicy::make(cacheSize);
  auto fbr = FBRCachingPolicy::make(cacheSize);

  // queries
  SqlGenerator sqlGenerator;
  auto sqls = sqlGenerator.generateSqlBatch(batchSize);

  // interpreters
  normal::sql::Interpreter i1(mode1, fbr);
  normal::sql::Interpreter i2(mode2, lru);
  normal::sql::Interpreter i3(mode3, fbr);
  configureS3ConnectorMultiPartition(i1, bucket_name, dir_prefix, partitionNum);
  configureS3ConnectorMultiPartition(i2, bucket_name, dir_prefix, partitionNum);
  configureS3ConnectorMultiPartition(i3, bucket_name, dir_prefix, partitionNum);

  // execute
  i1.boot();
  SPDLOG_INFO("Full-pushdown mode start");
  for (auto index = 1; index <= sqls.size(); ++index) {
    SPDLOG_INFO("sql {}", index);
    executeSql(i1, sqls[index - 1], true);
  }
  i1.stop();
  SPDLOG_INFO("Full-pushdown mode finished, metrics:\n{}", i1.showMetrics());

  i2.boot();
  SPDLOG_INFO("Pullup-caching mode start");
  for (auto index = 1; index <= sqls.size(); ++index) {
    SPDLOG_INFO("sql {}", index);
    executeSql(i2, sqls[index - 1], true);
  }
  i2.stop();
  SPDLOG_INFO("Pullup-caching mode finished, metrics:\n{}", i2.showMetrics());

  i3.boot();
  SPDLOG_INFO("Hybrid-caching mode start");
  for (auto index = 1; index <= sqls.size(); ++index) {
    SPDLOG_INFO("sql {}", index);
    executeSql(i3, sqls[index - 1], true);
  }
  i3.stop();
  SPDLOG_INFO("Hybrid-caching mode finished, metrics:\n{}", i3.showMetrics());

  SPDLOG_INFO("Cold-cache experiment finished, {} queries executed", batchSize);
}

TEST_CASE ("WarmCacheExperiment" * doctest::skip(true || SKIP_SUITE)) {
//  spdlog::set_level(spdlog::level::info);

  // parameters
  const int warmBatchSize = 10, executeBatchSize = 15;
  const size_t cacheSize = 1024*1024*300;
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf1/";
  const int partitionNum = 32;
  auto mode1 = normal::plan::operator_::mode::Modes::fullPushdownMode();
  auto mode2 = normal::plan::operator_::mode::Modes::pullupCachingMode();
  auto mode3 = normal::plan::operator_::mode::Modes::hybridCachingMode();
  auto lru = LRUCachingPolicy::make(cacheSize);
  auto fbr = FBRCachingPolicy::make(cacheSize);

  // queries
  SqlGenerator sqlGenerator;
  auto sqls = sqlGenerator.generateSqlBatch(warmBatchSize + executeBatchSize);

  // interpreters
  normal::sql::Interpreter i1(mode1, fbr);
  normal::sql::Interpreter i2(mode2, lru);
  normal::sql::Interpreter i3(mode3, fbr);
  configureS3ConnectorMultiPartition(i1, bucket_name, dir_prefix, partitionNum);
  configureS3ConnectorMultiPartition(i2, bucket_name, dir_prefix, partitionNum);
  configureS3ConnectorMultiPartition(i3, bucket_name, dir_prefix, partitionNum);

  // execute
//  i1.boot();
//  SPDLOG_INFO("Full-pushdown mode start");
//  SPDLOG_INFO("Cache warm phase:");
//  for (auto index = 1; index <= warmBatchSize; ++index) {
//    SPDLOG_INFO("sql {}", index);
//    executeSql(i1, sqls[index - 1], false);
//  }
//  SPDLOG_INFO("Cache warm phase finished");
//  SPDLOG_INFO("Execution phase:");
//  for (auto index = warmBatchSize + 1; index <= sqls.size(); ++index) {
//    SPDLOG_INFO("sql {}:\n{}", index - warmBatchSize, sqls[index - 1]);
//    executeSql(i1, sqls[index - 1], true);
//  }
//  SPDLOG_INFO("Execution phase finished");
//  i1.stop();
//  SPDLOG_INFO("Full-pushdown mode finished, metrics:\n{}", i1.showMetrics());
//
//  i2.boot();
//  SPDLOG_INFO("Pullup-caching mode start");
//  SPDLOG_INFO("Cache warm phase:");
//  for (auto index = 1; index <= warmBatchSize; ++index) {
//    SPDLOG_INFO("sql {}", index);
//    executeSql(i2, sqls[index - 1], false);
//  }
//  SPDLOG_INFO("Cache warm phase finished");
//  SPDLOG_INFO("Execution phase:");
//  for (auto index = warmBatchSize + 1; index <= sqls.size(); ++index) {
//    SPDLOG_INFO("sql {}:\n{}", index - warmBatchSize, sqls[index - 1]);
//    executeSql(i2, sqls[index - 1], true);
//  }
//  SPDLOG_INFO("Execution phase finished");
//  i2.stop();
//  SPDLOG_INFO("Pullup-caching mode finished, metrics:\n{}", i2.showMetrics());

  i3.boot();
  SPDLOG_INFO("Hybrid-caching mode start");
  SPDLOG_INFO("Cache warm phase:");
  for (auto index = 1; index <= warmBatchSize; ++index) {
    SPDLOG_INFO("sql {}", index);
    executeSql(i3, sqls[index - 1], false);
  }
  SPDLOG_INFO("Cache warm phase finished");
  SPDLOG_INFO("Execution phase:");
  for (auto index = warmBatchSize + 1; index <= sqls.size(); ++index) {
    SPDLOG_INFO("sql {}:\n{}", index - warmBatchSize, sqls[index - 1]);
    executeSql(i3, sqls[index - 1], true);
  }
  SPDLOG_INFO("Execution phase finished");
  i3.stop();
  SPDLOG_INFO("Hybrid-caching mode finished, metrics:\n{}", i3.showMetrics());

  SPDLOG_INFO("Warm-cache experiment finished, {} queries executed", executeBatchSize);
}

TEST_CASE ("t" * doctest::skip(false || SKIP_SUITE)) {
  int i = 0;
  char *p = NULL;
  while (true) {
    p = new char[1000000];
    i++;
    if (i % 100000 == 0) {
      SPDLOG_INFO("{}", i);
    }
  }
}

}