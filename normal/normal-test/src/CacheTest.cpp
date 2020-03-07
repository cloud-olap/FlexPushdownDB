//
// Created by matt on 7/3/20.
//

#include <string>
#include <memory>
#include <vector>
#include <cstdio>
#include <unistd.h>

#include <doctest/doctest.h>

#include "normal/pushdown/S3SelectScan.h"
#include "normal/pushdown/Collate.h"
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/AWSClient.h>
#include <normal/pushdown/aggregate/Sum.h>
#include "Globals.h"

/**
 * TODO: Throwing errors when issuing AWS requests, reported as a CRC error but suspect an auth issue. Skip for now.
 */
TEST_CASE ("S3SelectScan -> Sum -> Collate"
               * doctest::skip(true)) {

  normal::pushdown::AWSClient client;
  client.init();

  char buff[FILENAME_MAX];
  getcwd(buff, FILENAME_MAX);
  std::string current_working_dir(buff);

  SPDLOG_DEBUG("Current working dir: {}", current_working_dir);

  auto s3selectScan = std::make_shared<normal::pushdown::S3SelectScan>("s3SelectScan",
                                                                       "mit-caching",
                                                                       "test/a.tbl",
                                                                       "select  * from S3Object",
                                                                       "a",
                                                                       "all",
                                                                       client.defaultS3Client());

  auto sumExpr = std::make_shared<normal::pushdown::aggregate::Sum>("sum", "f5");
  auto expressions2 =
      std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  expressions2->push_back(sumExpr);

  auto aggregate = std::make_shared<normal::pushdown::Aggregate>("aggregate", expressions2);

  auto collate = std::make_shared<normal::pushdown::Collate>("collate");

  s3selectScan->produce(aggregate);
  aggregate->consume(s3selectScan);

  aggregate->produce(collate);
  collate->consume(aggregate);

  auto mgr = std::make_shared<OperatorManager>();

  mgr->put(s3selectScan);
  mgr->put(aggregate);
  mgr->put(collate);

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  auto val = std::stod(tuples->getValue("sum", 0));

      CHECK(tuples->numRows() == 1);
      CHECK(tuples->numColumns() == 1);
      CHECK(val == 4400227);

  mgr->stop();

  client.shutdown();
}
