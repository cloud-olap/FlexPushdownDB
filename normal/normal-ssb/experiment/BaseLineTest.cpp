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

  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(i.getOperatorManager()->getOperator("collate"))->tuples();

  i.getOperatorManager()->stop();

  return tuples;
}

auto executeSql(normal::sql::Interpreter i, const std::string &sql) {
  i.clearOperatorManager();
  i.parse(sql);

//  TestUtil::writeExecutionPlan(*i.getLogicalPlan());
//  TestUtil::writeExecutionPlan(*i.getOperatorManager());

  auto tuples = execute(i);

  auto tupleSet = TupleSet2::create(tuples);
  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  SPDLOG_INFO("Metrics:\n{}", i.getOperatorManager()->showMetrics());
  SPDLOG_INFO("Finish");

  return tupleSet;
}

TEST_CASE ("FullPushdown-SequentialRun" * doctest::skip(true || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);

  // hardcoded parameters
  std::vector<std::string> sql_file_names = {
          "query1.1.sql", "query1.2.sql", "query1.3.sql"
  };
  auto currentPath = filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql");
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf0.01/";

  // configure interpreter
  normal::sql::Interpreter i;
  configureS3Connector(i, bucket_name, dir_prefix);

  // execute
  for (const auto &sql_file_name: sql_file_names) {
    // read sql file
    auto sql_file_path = sql_file_dir_path.append(sql_file_name);
    SPDLOG_DEBUG(sql_file_dir_path.string());
    auto sql = ExperimentUtil::read_file(sql_file_path.string());
    SPDLOG_INFO("Sql: \n{}", sql);

    // execute sql
    executeSql(i, sql);
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }

  SPDLOG_INFO("Sequence all finished");
}

