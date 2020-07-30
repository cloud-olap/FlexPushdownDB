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
#include "ExperimentUtil.h"

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

void configureS3ConnectorMultiPartition(normal::sql::Interpreter &i, std::string bucket_name, std::string dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  // s3Objects map
  auto s3ObjectsMap = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>();
  auto lineorderS3Objects = std::make_shared<std::vector<std::string>>();
  for (int i = 0; i < 10; i++) {
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

auto executeSql(normal::sql::Interpreter i, const std::string &sql) {
  i.clearOperatorGraph();
  i.parse(sql);

  TestUtil::writeExecutionPlan(*i.getLogicalPlan());
  TestUtil::writeExecutionPlan2(*i.getOperatorGraph());

  auto tuples = execute(i);

  auto tupleSet = TupleSet2::create(tuples);
  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  SPDLOG_INFO("Metrics:\n{}", i.getOperatorGraph()->showMetrics());
  SPDLOG_INFO("Finish");

  return tupleSet;
}

TEST_CASE ("Baseline-SequentialRun" * doctest::skip(false || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);

  // chosse whether to use partitioned lineorder
  bool partitioned = true;

  // choose mode: FullPushDown or PullupCaching
  auto mode1 = normal::plan::operator_::mode::Modes::fullPushdownMode();
  auto mode2 = normal::plan::operator_::mode::Modes::pullupCachingMode();

  // hardcoded parameters
  std::vector<std::string> sql_file_names = {
          "query1.1.sql", "query1.2.sql", "query1.3.sql",
          "query2.1.sql", "query2.2.sql", "query2.3.sql",
          "query3.1.sql", "query3.2.sql", "query3.3.sql", "query3.4.sql",
          "query4.1.sql", "query4.2.sql", "query4.3.sql"
//"query4.1.sql","query4.2.sql","query4.3.sql"
  };
  auto currentPath = filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql");
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf0.01/";
  auto cachingPolicy = LRUCachingPolicy::make(100);

  // configure interpreter
  normal::sql::Interpreter i(mode2, cachingPolicy);
  if (partitioned) {
    configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix);
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
    executeSql(i, sql);
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }

  i.stop();

  SPDLOG_INFO("Sequence all finished");
}

