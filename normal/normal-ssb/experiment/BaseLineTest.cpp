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
#include "ExperimentUtil.h"

#define SKIP_SUITE false

using namespace normal::ssb;

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
//                                                                           partitioningScheme2,
//                                                                           cat));
//
//  i.put(cat);
//}

void configureS3Connector(normal::sql::Interpreter &i, std::string bucket_name, std::string dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  // look up tables
  std::vector<std::string> object_keys = ExperimentUtil::list_objects(*conn->getAwsClient(), bucket_name, dir_prefix);
  std::vector<std::string> table_names;
  for (auto object_key: object_keys) {
    std::string table_name = object_key.substr(dir_prefix.size(), object_key.size());
    if (table_name.size() > 0) {
      table_names.push_back(table_name);
    }
  }

  // configure s3Connector
  for (auto table_name: table_names) {
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

TEST_CASE ("FullPushdown-SequentialRun" * doctest::skip(false || SKIP_SUITE)) {
  // hardcoded parameters
  std::vector<std::string> sql_file_names = {
          "query1.1.sql"
  };
  auto currentPath = filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql");
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf1/";

  // configure interpreter
  normal::sql::Interpreter i;
  configureS3Connector(i, bucket_name, dir_prefix);

  // execute
  for (std::string sql_file_name: sql_file_names) {
    // read sql file
    auto sql_file_path = sql_file_dir_path.append(sql_file_name);
    auto sql = ExperimentUtil::read_file(sql_file_path.string());
    SPDLOG_INFO("Sql: \n{}", sql);

    // execute sql
    executeSql(i, sql);
  }


  SPDLOG_INFO("Finish");
}