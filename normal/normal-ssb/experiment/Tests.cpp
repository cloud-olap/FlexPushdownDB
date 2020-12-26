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
#include <normal/cache/FBRSCachingPolicy.h>
#include <normal/cache/WFBRCachingPolicy.h>
#include <normal/cache/BeladyCachingPolicy.h>
#include "ExperimentUtil.h"
#include "Tests.h"
#include <normal/ssb/SqlGenerator.h>
#include <normal/plan/Globals.h>
#include <normal/cache/Globals.h>
#include <normal/connector/MiniCatalogue.h>

#define SKIP_SUITE false

using namespace normal::ssb;

void generateSegmentKeyAndSqlQueryMappings(std::shared_ptr<normal::plan::operator_::mode::Mode> mode, std::shared_ptr<normal::cache::BeladyCachingPolicy> beladyCachingPolicy,
                                           std::string bucket_name, std::string dir_prefix, int numQueries, filesystem::path sql_file_dir_path);

void configureS3ConnectorSinglePartition(normal::sql::Interpreter &i, std::string bucket_name, std::string dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  // look up tables
  auto tableNames = normal::connector::defaultMiniCatalogue->tables();
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  for (const auto &tableName: *tableNames) {
    auto s3Object = dir_prefix + tableName + ".tbl";
    s3Objects->emplace_back(s3Object);
  }
  auto objectNumBytes_Map = normal::connector::s3::S3Util::listObjects(bucket_name, dir_prefix, *s3Objects, normal::plan::DefaultS3Client);

  // configure s3Connector
  for (int tbl_id = 0; tbl_id < tableNames->size(); tbl_id++) {
    auto &tableName = tableNames->at(tbl_id);
    auto &s3Object = s3Objects->at(tbl_id);
    auto numBytes = objectNumBytes_Map.find(s3Object)->second;
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    partitioningScheme->add(std::make_shared<S3SelectPartition>(bucket_name, s3Object, numBytes));
    cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
  }
  i.put(cat);
}

void configureS3ConnectorMultiPartition(normal::sql::Interpreter &i, std::string bucket_name, std::string dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  // get partitionNums
  auto s3ObjectsMap = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>();
  auto partitionNums = normal::connector::defaultMiniCatalogue->partitionNums();
  for (auto const &partitionNumEntry: *partitionNums) {
    auto tableName = partitionNumEntry.first;
    auto partitionNum = partitionNumEntry.second;
    auto objects = std::make_shared<std::vector<std::string>>();
    if (partitionNum == 1) {
      objects->emplace_back(dir_prefix + tableName + ".tbl");
      s3ObjectsMap->emplace(tableName, objects);
    } else {
      for (int i = 0; i < partitionNum; i++) {
        objects->emplace_back(fmt::format("{0}{1}_sharded/{1}.tbl.{2}", dir_prefix, tableName, i));
      }
      s3ObjectsMap->emplace(tableName, objects);
    }
  }

  // look up tables
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto objects = s3ObjectPair.second;
    s3Objects->insert(s3Objects->end(), objects->begin(), objects->end());
  }
  auto objectNumBytes_Map = normal::connector::s3::S3Util::listObjects(bucket_name, dir_prefix, *s3Objects, normal::plan::DefaultS3Client);

  // configure s3Connector
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto tableName = s3ObjectPair.first;
    auto objects = s3ObjectPair.second;
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    for (auto const &s3Object: *objects) {
      auto numBytes = objectNumBytes_Map.find(s3Object)->second;
      partitioningScheme->add(std::make_shared<S3SelectPartition>(bucket_name, s3Object, numBytes));
    }
    cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
  }
  i.put(cat);
}

auto execute(normal::sql::Interpreter &i) {
  i.getCachingPolicy()->onNewQuery();
  i.getOperatorGraph()->boot();
  i.getOperatorGraph()->start();
  i.getOperatorGraph()->join();

  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(i.getOperatorGraph()->getOperator("collate"))->tuples();

  return tuples;
}

auto executeSql(normal::sql::Interpreter &i, const std::string &sql, bool saveMetrics) {
//  i.getOperatorManager()->getSegmentCacheActor()->ctx()->operatorMap().clearForSegmentCache();
  i.clearOperatorGraph();
  i.parse(sql);

  // graph is too large
//  TestUtil::writeExecutionPlan(*i.getLogicalPlan());
//  TestUtil::writeExecutionPlan2(*i.getOperatorGraph());

  auto tuples = execute(i);

  auto tupleSet = TupleSet2::create(tuples);
//  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  if (saveMetrics)
  SPDLOG_INFO("Metrics:\n{}", i.getOperatorGraph()->showMetrics());
  SPDLOG_INFO("Finished, time: {} secs", (double) (i.getOperatorGraph()->getElapsedTime().value()) / 1000000000.0);
//  SPDLOG_INFO("Current cache layout:\n{}", i.getCachingPolicy()->showCurrentLayout());
  SPDLOG_INFO("Memory allocated: {}", arrow::default_memory_pool()->bytes_allocated());
  if (saveMetrics) {
    i.saveMetrics();
  }
  i.saveHitRatios();

  i.getOperatorGraph().reset();
//  std::this_thread::sleep_for (std::chrono::seconds(2));
  return tupleSet;
}

void normal::ssb::mainTest(size_t cacheSize, int modeType, int cachingPolicyType) {
  spdlog::set_level(spdlog::level::info);

  // parameters
  const int warmBatchSize = 30, executeBatchSize = 50;
  std::string bucket_name = "pushdowndb";
  std::string dir_prefix = "ssb-sf100-sortlineorder/csv/";
  normal::cache::beladyMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(bucket_name, dir_prefix);


  std::shared_ptr<normal::plan::operator_::mode::Mode> mode;
  std::string modeAlias;
  switch (modeType) {
    case 1: mode = normal::plan::operator_::mode::Modes::fullPullupMode(); modeAlias = "fpu"; break;
    case 2: mode = normal::plan::operator_::mode::Modes::fullPushdownMode(); modeAlias = "fpd"; break;
    case 3: mode = normal::plan::operator_::mode::Modes::pullupCachingMode(); modeAlias = "pc"; break;
    case 4: mode = normal::plan::operator_::mode::Modes::hybridCachingMode(); modeAlias = "hc"; break;
    default: throw std::runtime_error("Mode not found, type: " + std::to_string(modeType));
  }

  std::shared_ptr<normal::cache::CachingPolicy> cachingPolicy;
  std::string cachingPolicyAlias;
  switch (cachingPolicyType) {
    case 1: cachingPolicy = LRUCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "lru"; break;
    case 2: cachingPolicy = FBRSCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "lfu"; break;
    case 3: cachingPolicy = WFBRCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "wlfu"; break;
    case 4: cachingPolicy = BeladyCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "bldy"; break;
    default: throw std::runtime_error("CachingPolicy not found, type: " + std::to_string(cachingPolicyType));
  }

  auto currentPath = filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql/generated");

  if (cachingPolicy->id() == BELADY) {
    auto beladyCachingPolicy = std::static_pointer_cast<normal::cache::BeladyCachingPolicy>(cachingPolicy);
    generateSegmentKeyAndSqlQueryMappings(mode, beladyCachingPolicy, bucket_name, dir_prefix, warmBatchSize + executeBatchSize, sql_file_dir_path);
    // Generate caching decisions for belady
    SPDLOG_INFO("Generating belady caching decisions. . .");
    beladyCachingPolicy->generateCacheDecisions(warmBatchSize + executeBatchSize);
    SPDLOG_INFO("belady caching decisions generated");
//    SPDLOG_INFO("Belady caching decisions:\n" + beladyCachingPolicy->printLayoutAfterEveryQuery());
  }

  // interpreter
  normal::sql::Interpreter i(mode, cachingPolicy);
  configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix);

  // execute
  i.boot();
  SPDLOG_INFO("{} mode start", mode->toString());
  if (mode->id() != normal::plan::operator_::mode::ModeId::FullPullup &&
      mode->id() != normal::plan::operator_::mode::ModeId::FullPushdown) {
    SPDLOG_INFO("Cache warm phase:");
    for (auto index = 1; index <= warmBatchSize; ++index) {
      SPDLOG_INFO("sql {}", index);
      if (cachingPolicy->id() == BELADY) {
        normal::cache::beladyMiniCatalogue->setCurrentQueryNum(index);
      }
      auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", index));
      auto sql = ExperimentUtil::read_file(sql_file_path.string());
      executeSql(i, sql, true);
      sql_file_dir_path = sql_file_dir_path.parent_path();
    }
    SPDLOG_INFO("Cache warm phase finished");
  }

  // collect warmup metrics for later output
  std::string warmupMetrics = i.showMetrics();
  std::string warmupCacheMetrics = i.getOperatorManager()->showCacheMetrics();
  i.clearMetrics();

  i.getOperatorManager()->clearCacheMetrics();

  SPDLOG_INFO("Execution phase:");
  for (auto index = warmBatchSize + 1; index <= warmBatchSize + executeBatchSize; ++index) {
    SPDLOG_INFO("sql {}", index - warmBatchSize);
    if (cachingPolicy->id() == BELADY) {
      normal::cache::beladyMiniCatalogue->setCurrentQueryNum(index);
    }
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", index));
    auto sql = ExperimentUtil::read_file(sql_file_path.string());
    executeSql(i, sql, true);
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }
  SPDLOG_INFO("Execution phase finished");

  SPDLOG_INFO("{} mode finished\nExecution metrics:\n{}", mode->toString(), i.showMetrics());
  SPDLOG_INFO("Cache Metrics:\n{}", i.getOperatorManager()->showCacheMetrics());
  SPDLOG_INFO("Cache hit ratios:\n{}", i.showHitRatios());
    SPDLOG_INFO("OnLoad time: {}", i.getCachingPolicy()->onLoadTime);
    SPDLOG_INFO("OnStore time: {}", i.getCachingPolicy()->onStoreTime);
    SPDLOG_INFO("OnToCache time: {}", i.getCachingPolicy()->onToCacheTime);

  auto metricsFilePath = filesystem::current_path().append("metrics-" + modeAlias + "-" + cachingPolicyAlias);
  std::ofstream fout(metricsFilePath.string());
  fout << mode->toString() << " mode finished\n";
  fout << "Warmup metrics:\n" << warmupMetrics << "\n";
  fout << "Warmup Cache metrics:\n" << warmupCacheMetrics << "\n";
  fout << "Execution metrics:\n" << i.showMetrics() << "\n";
  fout << "Execution Cache metrics:\n" << i.getOperatorManager()->showCacheMetrics() << "\n";
  fout << "All Cache hit ratios:\n" << i.showHitRatios() << "\n";
//  fout << "Current cache layout:\n" << i.getCachingPolicy()->showCurrentLayout() << "\n";
  fout.flush();
  fout.close();

  i.getOperatorGraph().reset();
  i.stop();
  SPDLOG_INFO("Memory allocated finally: {}", arrow::default_memory_pool()->bytes_allocated());
}

TEST_SUITE ("MainTests" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("SequentialRun" * doctest::skip(true || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);

  // choose whether to use partitioned lineorder
  bool partitioned = true;

  // choose mode
  auto mode0 = normal::plan::operator_::mode::Modes::fullPullupMode();
  auto mode1 = normal::plan::operator_::mode::Modes::fullPushdownMode();
  auto mode2 = normal::plan::operator_::mode::Modes::pullupCachingMode();
  auto mode3 = normal::plan::operator_::mode::Modes::hybridCachingMode();
  auto mode4 = normal::plan::operator_::mode::Modes::hybridCachingLastMode();

  // hardcoded parameters
  std::vector<std::string> sql_file_names = {
          "query1.1.sql", "query1.2.sql", "query1.3.sql",
          "query2.1.sql", "query2.2.sql", "query2.3.sql",
          "query3.1.sql", "query3.2.sql", "query3.3.sql", "query3.4.sql",
          "query4.1.sql", "query4.2.sql", "query4.3.sql"
//          "query3.1.sql"
  };
  std::vector<int> order1 = {
          0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
  };
  std::vector<int> order2 = {
          0, 7, 12, 4, 11, 1, 3, 10, 8, 2, 9, 5, 6
  };
//  order1.insert(order1.end(), order1.begin(), order1.end());
//  order1.insert(order1.end(), order1.begin(), order1.end());
//  order1.insert(order1.end(), order1.begin(), order1.end());
  auto currentPath = filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql/filterlineorder");
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf1/";

  auto chosenMode = mode3;

  // choose caching policy
  auto lru = LRUCachingPolicy::make(1024*1024*300, chosenMode);
  auto fbr = FBRCachingPolicy::make(1024*1024*300, chosenMode);

  // configure interpreter
  normal::sql::Interpreter i(chosenMode, fbr);
  if (partitioned) {
    configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix);
  } else {
    configureS3ConnectorSinglePartition(i, bucket_name, dir_prefix);
  }

  // execute
  i.boot();
  int cnt = 1;
  for (const auto index: order2) {
    // read sql file
    auto sql_file_name = sql_file_names[index];
    auto sql_file_path = sql_file_dir_path.append(sql_file_name);
    SPDLOG_DEBUG(sql_file_dir_path.string());
    auto sql = ExperimentUtil::read_file(sql_file_path.string());
    SPDLOG_INFO("{}-{}: \n{}", cnt++, sql_file_name, sql);

    // execute sql
    executeSql(i, sql, true);
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }

  SPDLOG_INFO("Sequence all finished");
  SPDLOG_INFO("Overall Metrics:\n{}", i.showMetrics());
  SPDLOG_INFO("Cache Metrics:\n{}", i.getOperatorManager()->showCacheMetrics());

  i.stop();
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
    configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix);
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

TEST_CASE ("WarmCacheExperiment-Single" * doctest::skip(false || SKIP_SUITE)) {
//  size_t cacheSize = 1024L*1024*1024;
//  int modeType = 4;
//  int cachingPolicyType = 2;
//  mainTest(cacheSize, modeType, cachingPolicyType);
}
}
