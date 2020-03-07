//
// Created by matt on 5/3/20.
//

#include <string>
#include <memory>
#include <vector>
#include <cstdio>
#include <unistd.h>

#include <doctest/doctest.h>

#include "normal/pushdown/Collate.h"
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/FileScan.h>
#include <normal/pushdown/aggregate/Sum.h>
#include "Globals.h"

TEST_CASE ("FileScan -> Sum -> Collate") {

  char buff[FILENAME_MAX];
  getcwd(buff, FILENAME_MAX);
  std::string current_working_dir(buff);

  SPDLOG_DEBUG("Current working dir: {}", current_working_dir);

  auto fileScan = std::make_shared<normal::pushdown::FileScan>("fileScan", "data/test.csv");

  auto sumExpr = std::make_shared<normal::pushdown::aggregate::Sum>("Sum", "A");
  auto
      expressions = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  expressions->emplace_back(sumExpr);

  auto aggregate = std::make_shared<normal::pushdown::Aggregate>("aggregate", expressions);
  auto collate = std::make_shared<normal::pushdown::Collate>("collate");

  fileScan->produce(aggregate);
  aggregate->consume(fileScan);

  aggregate->produce(collate);
  collate->consume(aggregate);

  auto mgr = std::make_shared<OperatorManager>();

  mgr->put(fileScan);
  mgr->put(aggregate);
  mgr->put(collate);

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  auto val = std::stod(tuples->getValue("Sum", 0));

      CHECK(tuples->numRows() == 1);
      CHECK(tuples->numColumns() == 1);
      CHECK(val == 12);

  mgr->stop();
}