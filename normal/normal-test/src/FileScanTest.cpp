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
#include "normal/core/expression/Expression.h"

TEST_CASE ("FileScan -> Sum -> Collate"
               * doctest::skip(true)) {

  char buff[FILENAME_MAX];
  getcwd(buff, FILENAME_MAX);
  std::string current_working_dir(buff);

  SPDLOG_DEBUG("Current working dir: {}", current_working_dir);

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto fileScan = std::make_shared<normal::pushdown::FileScan>("fileScan", "data/data-file-simple/test.csv");

  auto expression = Expression("A").plus(Expression("B"))



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

  mgr->put(fileScan);
  mgr->put(aggregate);
  mgr->put(collate);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  auto val = std::stod(tuples->getValue("Sum", 0));

      CHECK(tuples->numRows() == 1);
      CHECK(tuples->numColumns() == 1);
      CHECK(val == 12);

  mgr->stop();
}

TEST_CASE ("Sharded FileScan -> Sum -> Collate"
               * doctest::skip(true)) {

  char buff[FILENAME_MAX];
  getcwd(buff, FILENAME_MAX);
  std::string current_working_dir(buff);

  SPDLOG_DEBUG("Current working dir: {}", current_working_dir);

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto fileScan01 = std::make_shared<normal::pushdown::FileScan>("fileScan01", "data/data-file-sharded/test01.csv");
  auto fileScan02 = std::make_shared<normal::pushdown::FileScan>("fileScan02", "data/data-file-sharded/test02.csv");
  auto fileScan03 = std::make_shared<normal::pushdown::FileScan>("fileScan03", "data/data-file-sharded/test03.csv");

  auto sumExpr01 = std::make_shared<normal::pushdown::aggregate::Sum>("sum(A)", "A");
  auto
      expressions01 =
      std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  expressions01->emplace_back(sumExpr01);
  auto aggregate01 = std::make_shared<normal::pushdown::Aggregate>("aggregate01", expressions01);

  auto sumExpr02 = std::make_shared<normal::pushdown::aggregate::Sum>("sum(A)", "A");
  auto
      expressions02 =
      std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  expressions02->emplace_back(sumExpr02);
  auto aggregate02 = std::make_shared<normal::pushdown::Aggregate>("aggregate02", expressions02);

  auto sumExpr03 = std::make_shared<normal::pushdown::aggregate::Sum>("sum(A)", "A");
  auto
      expressions03 =
      std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  expressions03->emplace_back(sumExpr03);
  auto aggregate03 = std::make_shared<normal::pushdown::Aggregate>("aggregate03", expressions03);

  auto reduceSumExpr = std::make_shared<normal::pushdown::aggregate::Sum>("sum(A)", "sum(A)");
  auto
      reduceAggregateExpressions =
      std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  reduceAggregateExpressions->emplace_back(reduceSumExpr);
  auto reduceAggregate = std::make_shared<normal::pushdown::Aggregate>("reduceAggregate", reduceAggregateExpressions);

  auto collate = std::make_shared<normal::pushdown::Collate>("collate");

  fileScan01->produce(aggregate01);
  aggregate01->consume(fileScan01);

  fileScan02->produce(aggregate02);
  aggregate02->consume(fileScan02);

  fileScan03->produce(aggregate03);
  aggregate03->consume(fileScan03);

  aggregate01->produce(reduceAggregate);
  reduceAggregate->consume(aggregate01);

  aggregate02->produce(reduceAggregate);
  reduceAggregate->consume(aggregate02);

  aggregate03->produce(reduceAggregate);
  reduceAggregate->consume(aggregate03);

  reduceAggregate->produce(collate);
  collate->consume(reduceAggregate);

  mgr->put(fileScan01);
  mgr->put(fileScan02);
  mgr->put(fileScan03);
  mgr->put(aggregate01);
  mgr->put(aggregate02);
  mgr->put(aggregate03);
  mgr->put(reduceAggregate);
  mgr->put(collate);

  SPDLOG_DEBUG("Writing plan to: {}", current_working_dir + "/test/plan.svg");

  mgr->write_graph(current_working_dir + "/test/plan.svg");

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  auto val = std::stod(tuples->getValue("sum(A)", 0));

      CHECK(tuples->numRows() == 1);
      CHECK(tuples->numColumns() == 1);
      CHECK(val == 36);

  mgr->stop();

}